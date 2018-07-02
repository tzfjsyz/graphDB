const cacheHandlers = require('./cacheHandlers.js');
const searchGraph = require('./searchGraph.js');
const schedule = require("node-schedule");
const moment = require('moment');
const log4js = require('log4js');
// const Promise = require('bluebird');
const req = require('require-yml');
const config = req('./config/source.yml');
const path = require('path');
const Redis = require('ioredis');
const redis = new Redis(config.lockInfo.redisUrl[0]);
const redis1 = new Redis(config.lockInfo.redisUrl[1]);
const Redlock = require('redlock');
const lockResource = config.lockInfo.resource[0];
const lockTTL = config.lockInfo.TTL[0];
console.log('lockResource: ' + lockResource + ', lockTTL: ' + lockTTL + ' ms');
let redlock = new Redlock(
    // you should have one client for each independent redis node
    // or cluster
    [redis, redis1],
    {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01, // time in ms

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount: 1000,

        // the time in ms between attempts
        retryDelay: 10000, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter: 200 // time in ms
    }
);

const errorCode = {
    ARG_ERROR: {
        code: -100,
        msg: "请求参数错误"
    },
    NOTSUPPORT: {
        code: -101,
        msg: "不支持的参数"
    },
    INTERNALERROR: {
        code: -200,
        msg: "服务内部错误"
    },
    NOMATCHINGDATA: {
        code: -505,
        msg: "无匹配数据"
    }
}

function errorResp(err, msg) {
    return { ok: err.code, error: msg || err.msg };
}

// log4js.configure({
//     appenders: {
//         'out': {
//             type: 'file',         //文件输出
//             filename: 'logs/queryDataInfo.log',
//             maxLogSize: config.logInfo.maxLogSize
//         }
//     },
//     categories: { default: { appenders: ['out'], level: 'info' } }
// });
// const logger = log4js.getLogger();
log4js.configure({
    // appenders: {
    //     'out': {
    //         type: 'file',         //文件输出
    //         filename: 'logs/queryDataInfo.log',
    //         maxLogSize: config.logInfo.maxLogSize
    //     }
    // },
    // categories: { default: { appenders: ['out'], level: 'info' } }
    appenders: {
        console: {
            type: 'console'
        },
        log: {
            type: "dateFile",
            filename: "./logs/log4js_log-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize
        },
        error: {
            type: "dateFile",
            filename: "./logs/log4js_err-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize
        },
        errorFilter: {
            type: "logLevelFilter",
            appender: "error",
            level: "error"
        },
    },
    categories: {
        default: { appenders: ['console', 'log', 'errorFilter'], level: 'info' }
    },
    pm2: true,
    pm2InstanceVar: 'INSTANCE_ID'
});
const logger = log4js.getLogger('graphPath_search_terminal');

//定时主动预热paths
var rule = new schedule.RecurrenceRule();
rule.dayOfWeek = [0, new schedule.Range(1, 6)];
rule.hour = config.schedule.hour;
rule.minute = config.schedule.minute;
console.log('定时主动预热paths时间: ' + rule.hour + '时 ' + rule.minute + '分');
// logger.info('定时主动预热paths时间: ' + rule.hour + '时 ' + rule.minute + '分');
schedule.scheduleJob(rule, function () {
    redlock.lock(lockResource, lockTTL).then(async function (lock) {
        timingWarmUpPaths('true');
        redlock.on('clientError', function (err) {
            console.error('A redis error has occurred:', err);
        });
    });
});

//定时触发主动查询需要预热的path数据
async function timingWarmUpPaths(flag) {
    try {
        if (flag) {
            let conditionsResult = await lookUpConditionsFieldInfo(null);
            let conditionsFields = conditionsResult.conditionsField;
            if (conditionsFields.length > 0) {
                for (let subField of conditionsFields) {
                    let subFieldArr = subField.toString().split('->');
                    let from = subFieldArr[0];
                    let to = subFieldArr[1];
                    await addQueryDataInfo(from, to);
                }
            }
            else if (conditionsFields.length == 0) {
                console.log('no paths to warm up!');
            }
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
    }
}

//内部方法调用, 查询所有的预热数据的key对应的field
async function lookUpConditionsFieldInfo(conditionsKey) {
    try {
        if (!conditionsKey) conditionsKey = config.redisKeyName.conditionsKey;
        let conditionsField = await cacheHandlers.findWarmUpConditionsField(conditionsKey);
        if (!conditionsField) {
            return ({ conditionsKey: conditionsKey, conditionsField: [], fieldsNum: 0 });
        }
        else if (conditionsField) {
            return ({ conditionsKey: conditionsKey, conditionsField: conditionsField, fieldsNum: conditionsField.length });
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
}

//内部方法调用, 添加需要预加热的查询条件信息
async function addQueryDataInfo(codeOne, codeTwo) {
    let IVDepth = config.pathDepth.IVDepth;                              //直接投资路径查询深度
    let IVBDepth = config.pathDepth.IVBDepth;                            //直接被投资路径查询深度
    let FUDepth = config.pathDepth.FUDepth;                              //全路径查询深度
    let CIVDepth = config.pathDepth.CIVDepth;                            //共同投资关系路径查询深度
    let CIVBDepth = config.pathDepth.CIVBDepth;                          //共同被投资关系路径查询深度
    try {
        if (codeOne && codeTwo) {
            let now = Date.now();
            console.log('warmUpData  from: ' + codeOne + ', to: ' + codeTwo);
            logger.info('warmUpData  from: ' + codeOne + ', to: ' + codeTwo);
            //1、先将符合预热条件的数据存到redis中
            let conditionsKey = config.redisKeyName.conditionsKey;
            let conditionsField = [codeOne, codeTwo].join('->');
            let conditionsValue = { from: codeOne, to: codeTwo, depth: [IVDepth, IVBDepth, CIVDepth, CIVBDepth], relations: [0, 1, 4, 5] };
            cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
            //2、再将先前符合预热条件的conditionsValue作为key去筛选path, path作为value存入redis
            for (let subRelation of conditionsValue.relations) {
                if (subRelation == 0) {
                    // let warmUpKey = [conditionsValue.from, conditionsValue.to, conditionsValue.depth[0], subRelation].join('-');
                    await searchGraph.queryInvestPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[0], 0, 100);
                }
                else if (subRelation == 1) {
                    await searchGraph.queryInvestedByPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[1], 0, 100);
                }
                // else if (subRelation == 3) {
                //     searchGraph.queryfullPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[2], 0, 100);
                // }
                else if (subRelation == 4) {
                    await searchGraph.queryCommonInvestPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[2], 0, 100);
                }
                else if (subRelation == 5) {
                    await searchGraph.queryCommonInvestedByPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[3], 0, 100);
                }
            }
            console.log('addWarmUpQueryDataInfo: ok');
            logger.info('addWarmUpQueryDataInfo: ok');

        } else if (!codeOne && codeTwo) {
            console.error(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            logger.error(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
        } else if (codeOne && !codeTwo) {
            console.errore(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            logger.error(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
        } else if (!codeOne && !codeTwo) {
            console.error(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            logger.error(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
    }
}

let apiHandlers = {

    //多种方式路径同时查询
    // queryInfo: async function (request, reply) {
    //     // let nameOne =  request.query.from;
    //     // let nameTwo =  request.query.to;
    //     let codeOne = request.query.from;
    //     let codeTwo = request.query.to;
    //     let lowWeight = request.query.lowWeight;                           //最低投资比例
    //     let highWeight = request.query.highWeight;                         //最高投资比例
    //     if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
    //     if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
    //     let lowFund = request.query.lowFund;                               //最低注册资金
    //     let highFund = request.query.highFund;                             //最高注册资金
    //     if (lowWeight) lowWeight = parseFloat(lowWeight);
    //     if (highWeight) highWeight = parseFloat(highWeight);
    //     // let num = request.query.pathNum;
    //     let IVDepth = request.query.investPathDepth;
    //     let IVBDepth = request.query.investedByPathDepth;
    //     // let CIVDepth = request.query.comInvestPathDepth;
    //     // let CIVBDepth = request.query.comInvestedByPathDepth;
    //     let FUDepth = request.query.fullPathDepth;
    //     // let allPath = request.query.allPath;
    //     //测试数据，大智慧到招商证券
    //     // let codeOne = '1005213985';
    //     // let codeTwo = '1012419137';
    //     if (!IVDepth) IVDepth = 6;
    //     if (!IVBDepth) IVBDepth = 6;
    //     // if (!CIVDepth) CIVDepth = 3;
    //     // if (!CIVBDepth) CIVBDepth = 3;
    //     if (!FUDepth) FUDepth = 3;
    //     try {
    //         if (lowWeight && highWeight && lowWeight > highWeight) {
    //             return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
    //         }
    //         let res = null;
    //         if (codeOne && codeTwo) {

    //             // let codeOne = await searchGraph.nameToCode(nameOne);
    //             // let codeTwo = await searchGraph.nameToCode(nameTwo);
    //             let now = Date.now();

    //             searchGraph.queryNodePath(codeOne, codeTwo, IVDepth, IVBDepth, FUDepth, lowWeight, highWeight)
    //                 .then(res => {
    //                     let totalQueryCost = Date.now() - now;
    //                     logger.info("TotalQueryCost: " + totalQueryCost + 'ms');
    //                     console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", TotalQueryCost: " + totalQueryCost + 'ms');

    //                     if (!res)
    //                         return reply.response({ direction: { from: codeOne, to: codeTwo }, results: "no results!", totalPathNum: 0 });
    //                     else reply.response({ direction: { from: codeOne, to: codeTwo }, results: res.dataDetail, totalPathNum: res.totalPathNum });
    //                 }).catch(err => {
    //                     return reply.response({ ok: -1, message: err.message || err });
    //                 });

    //         } else if (!codeOne && codeTwo) {
    //             return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
    //         } else if (codeOne && !codeTwo) {
    //             return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
    //         } else if (!codeOne && !codeTwo) {
    //             return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
    //         }

    //     } catch (err) {
    //         return reply.response(err);
    //     }
    // },

    //直接投资关系路径
    queryInvestPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let IVDepth = request.query.investPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!IVDepth) IVDepth = config.pathDepth.IVDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    let queryCodeOne = codeOne;
                    let queryCodeTwo = codeTwo;
                    //判断codeOne、codeTwo是否自然人的personalCode
                    if (codeOne.slice(0, 1) == 'P') {
                        queryCodeOne = parseInt(codeOne.replace(/P/g, ''));
                    }
                    if (codeTwo.slice(0, 1) == 'P') {
                        queryCodeTwo = parseInt(codeTwo.replace(/P/g, ''));
                    }
                    searchGraph.queryInvestPath(queryCodeOne, queryCodeTwo, IVDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryInvestPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!" } });
                            }
                            else if (res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail } });
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //直接被投资关系路径
    queryInvestedByPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let IVBDepth = request.query.investedByPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!IVBDepth) IVBDepth = config.pathDepth.IVBDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    let queryCodeOne = codeOne;
                    let queryCodeTwo = codeTwo;
                    //判断codeOne、codeTwo是否自然人的personalCode
                    if (codeOne.slice(0, 1) == 'P') {
                        queryCodeOne = parseInt(codeOne.replace(/P/g, ''));
                    }
                    if (codeTwo.slice(0, 1) == 'P') {
                        queryCodeTwo = parseInt(codeTwo.replace(/P/g, ''));
                    }
                    searchGraph.queryInvestedByPath(queryCodeOne, queryCodeTwo, IVBDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!" } });
                            }
                            else if (res) {
                                // let totalPathNum = res.pathDetail.data.pathNum;
                                // console.log(`from: ${codeOne} to: ${codeTwo}`+ ' queryInvestPath_totalPathNum: ' + totalPathNum);
                                // logger.info(`from: ${codeOne} to: ${codeTwo}`+ ' queryInvestPath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail } });
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //最短路径
    queryShortestPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    let queryCodeOne = codeOne;
                    let queryCodeTwo = codeTwo;
                    //判断codeOne、codeTwo是否自然人的personalCode
                    if (codeOne.slice(0, 1) == 'P') {
                        queryCodeOne = parseInt(codeOne.replace(/P/g, ''));
                    }
                    if (codeTwo.slice(0, 1) == 'P') {
                        queryCodeTwo = parseInt(codeTwo.replace(/P/g, ''));
                    }
                    searchGraph.queryShortestPath(queryCodeOne, queryCodeTwo, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryShortestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryShortestPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!" } });
                            }
                            else if (res) {
                                // let totalPathNum = res.pathDetail.data.pathNum;
                                // console.log(`from: ${codeOne} to: ${codeTwo}`+ ' queryInvestPath_totalPathNum: ' + totalPathNum);
                                // logger.info(`from: ${codeOne} to: ${codeTwo}`+ ' queryInvestPath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail } });
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //全部路径
    queryFullPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let FUDepth = request.query.fullPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!FUDepth) FUDepth = config.pathDepth.FUDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryFullPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryFullPath  from: ' + codeOne + ', to: ' + codeTwo);
                    let queryCodeOne = codeOne;
                    let queryCodeTwo = codeTwo;
                    //判断codeOne、codeTwo是否自然人的personalCode
                    if (codeOne.slice(0, 1) == 'P') {
                        queryCodeOne = parseInt(codeOne.replace(/P/g, ''));
                    }
                    if (codeTwo.slice(0, 1) == 'P') {
                        queryCodeTwo = parseInt(codeTwo.replace(/P/g, ''));
                    }
                    searchGraph.queryfullPath(queryCodeOne, queryCodeTwo, FUDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryFullPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryFullPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!" } });
                            }
                            else if (res) {
                                // let totalPathNum = res.pathDetail.data.pathNum;
                                // console.log(`from: ${codeOne} to: ${codeTwo}`+ ' queryFullPath_totalPathNum: ' + totalPathNum);
                                // logger.info(`from: ${codeOne} to: ${codeTwo}`+ ' queryFullPath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail } });
                            }

                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //共同投资关系路径
    queryCommonInvestPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let CIVDepth = request.query.comInvestPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!CIVDepth) CIVDepth = config.pathDepth.CIVDepth;
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryCommonInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryCommonInvestPath  from: ' + codeOne + ', to: ' + codeTwo);
                    let queryCodeOne = codeOne;
                    let queryCodeTwo = codeTwo;
                    //判断codeOne、codeTwo是否自然人的personalCode
                    if (codeOne.slice(0, 1) == 'P') {
                        queryCodeOne = parseInt(codeOne.replace(/P/g, ''));
                    }
                    if (codeTwo.slice(0, 1) == 'P') {
                        queryCodeTwo = parseInt(codeTwo.replace(/P/g, ''));
                    }
                    searchGraph.queryCommonInvestPath(queryCodeOne, queryCodeTwo, CIVDepth, lowWeight, highWeight, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryCommonInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryCommonInvestPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!" } });
                            }
                            else if (res) {
                                // let totalPathNum = res.pathDetail.data.pathNum;
                                // console.log(`from: ${codeOne} to: ${codeTwo}`+ ' queryCommonInvestPath_totalPathNum: ' + totalPathNum);
                                // logger.info(`from: ${codeOne} to: ${codeTwo}`+ ' queryCommonInvestPath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail } });
                            }

                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //共同被投资关系路径
    queryCommonInvestedByPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let CIVBDepth = request.query.comInvestedByPathDepth;
        let lowWeight = request.query.lowWeight;                           //最低投资比例
        let highWeight = request.query.highWeight;                         //最高投资比例
        if (lowWeight) lowWeight = parseFloat(lowWeight);
        if (highWeight) highWeight = parseFloat(highWeight);
        if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
        if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
        if (!CIVBDepth) CIVBDepth = config.pathDepth.CIVBDepth;
        let returnNoCodeNodes = request.query.returnNoCodeNodes;           //是否带入没有机构代码的节点查询
        if (returnNoCodeNodes == "false" || !returnNoCodeNodes) {          //不返回无机构代码的nodes, 即isExtra=0的nodes
            isExtra = 0;
        } else if (returnNoCodeNodes == "true") {                          //返回无机构代码的nodes, 即isExtra=1和isExtra=0的nodes
            isExtra = 1;
        } else {
            isExtra = 0;
        }
        let pathType = request.query.pathType;                             //默认返回的路径方式为invests
        if (!pathType) pathType = 'invests';
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            if (lowWeight && highWeight && lowWeight > highWeight) {
                return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
            }
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryCommonInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryCommonInvestedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    let queryCodeOne = codeOne;
                    let queryCodeTwo = codeTwo;
                    //判断codeOne、codeTwo是否自然人的personalCode
                    if (codeOne.slice(0, 1) == 'P') {
                        queryCodeOne = parseInt(codeOne.replace(/P/g, ''));
                    }
                    if (codeTwo.slice(0, 1) == 'P') {
                        queryCodeTwo = parseInt(codeTwo.replace(/P/g, ''));
                    }
                    searchGraph.queryCommonInvestedByPath(queryCodeOne, queryCodeTwo, CIVBDepth, lowWeight, highWeight, isExtra, pathType)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryCommonInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryCommonInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: "no results!", pathTypeTwo: "no results!" } });
                            }
                            else if (res) {
                                // let totalPathNum = res.pathDetail.data.pathNum;
                                // console.log(`from: ${codeOne} to: ${codeTwo}`+ ' queryCommonInvestedByPath_totalPathNum: ' + totalPathNum);
                                // logger.info(`from: ${codeOne} to: ${codeTwo}`+ ' queryCommonInvestedByPath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: { pathTypeOne: res.nodeResultOne.pathDetail, pathTypeTwo: res.nodeResultTwo.pathDetail } });
                            }

                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //清空缓存
    deleteCacheData: async function (request, reply) {
        try {
            cacheHandlers.flushCache();
            return reply.response({ ok: 1, message: 'flush the cache succeess!' });
        } catch (err) {
            console.error(err);
            logger.error(err);
            return reply.response(err);
        }
    },

    //单个企业直接投资关系路径查询
    // queryDirectInvestPathInfo: async function (request, reply) {
    //     let code = request.query.code;
    //     let DIDepth = request.query.directInvestPathDepth;
    //     let lowWeight = request.query.lowWeight;                           //最低投资比例
    //     let highWeight = request.query.highWeight;                         //最高投资比例
    //     let lowFund = request.query.lowFund;                               //最低注册资金
    //     let highFund = request.query.highFund;                             //最高注册资金
    //     if (lowWeight) lowWeight = parseFloat(lowWeight);
    //     if (highWeight) highWeight = parseFloat(highWeight);
    //     if (lowFund) lowFund = parseFloat(lowFund);
    //     if (highFund) highFund = parseFloat(highFund);
    //     if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
    //     if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
    //     if (!DIDepth) DIDepth = config.pathDepth.DIDepth;
    //     try {
    //         if (lowWeight && highWeight && lowWeight > highWeight) {
    //             return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
    //         }
    //         if (lowFund && highFund && lowFund > highFund) {
    //             return reply.response({ ok: -1, message: 'highFund must >= lowFund!' });
    //         }
    //         let res = null;
    //         if (code) {
    //             let now = Date.now();
    //             console.log('queryDirectInvestPath  code: ' + code);
    //             logger.info('queryDirectInvestPath  code: ' + code);
    //             searchGraph.queryDirectInvestPath(code, DIDepth, lowWeight, highWeight, lowFund, highFund)
    //                 .then(res => {
    //                     let totalQueryCost = Date.now() - now;
    //                     logger.info("queryDirectInvestPath_totalQueryCost: " + totalQueryCost + 'ms');
    //                     console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", queryDirectInvestPath_totalQueryCost: " + totalQueryCost + 'ms');

    //                     if (!res) {
    //                         return reply.response({ code: code, results: "no results!" });
    //                     }
    //                     else if (res) {
    //                         let totalPathNum = res.pathDetail.data.pathNum;
    //                         console.log('queryDirectInvestPath_totalPathNum: ' + totalPathNum);
    //                         logger.info('queryDirectInvestPath_totalPathNum: ' + totalPathNum);
    //                         return reply.response({ code: code, results: res.pathDetail });
    //                     }
    //                 }).catch(err => {
    //                     return reply.response({ ok: -1, message: err.message || err });
    //                 });
    //         } else if (!code) {
    //             return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
    //         }
    //     } catch (err) {
    //         return reply.response(err);
    //     }
    // },

    // //单个企业直接被投资关系路径查询
    // queryDirectInvestedByPathInfo: async function (request, reply) {
    //     let code = request.query.code;
    //     let DIBDepth = request.query.directInvestedByPathDepth;
    //     let lowWeight = request.query.lowWeight;                           //最低投资比例
    //     let highWeight = request.query.highWeight;                         //最高投资比例
    //     let lowFund = request.query.lowFund;                               //最低注册资金
    //     let highFund = request.query.highFund;                             //最高注册资金
    //     if (lowWeight) lowWeight = parseFloat(lowWeight);
    //     if (highWeight) highWeight = parseFloat(highWeight);
    //     if (lowFund) lowFund = parseFloat(lowFund);
    //     if (highFund) highFund = parseFloat(highFund);
    //     if (!lowWeight) lowWeight = 0;                                     //默认最低投资比例为0
    //     if (!highWeight) highWeight = 100;                                 //默认最高投资比例为100
    //     if (!DIBDepth) DIBDepth = config.pathDepth.DIBDepth;
    //     try {
    //         if (lowWeight && highWeight && lowWeight > highWeight) {
    //             return reply.response({ ok: -1, message: 'highWeight must >= lowWeight!' });
    //         }
    //         if (lowFund && highFund && lowFund > highFund) {
    //             return reply.response({ ok: -1, message: 'highFund must >= lowFund!' });
    //         }
    //         let res = null;
    //         if (code) {
    //             let now = Date.now();
    //             console.log('queryDirectInvestedByPath  code: ' + code);
    //             logger.info('queryDirectInvestedByPath  code: ' + code);
    //             searchGraph.queryDirectInvestedByPath(code, DIBDepth, lowWeight, highWeight, lowFund, highFund)
    //                 .then(res => {
    //                     let totalQueryCost = Date.now() - now;
    //                     logger.info("queryDirectInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');
    //                     console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", queryDirectInvestedByPath_totalQueryCost: " + totalQueryCost + 'ms');

    //                     if (!res) {
    //                         return reply.response({ code: code, results: "no results!" });
    //                     }
    //                     else if (res) {
    //                         let totalPathNum = res.pathDetail.data.pathNum;
    //                         console.log('queryDirectInvestPath_totalPathNum: ' + totalPathNum);
    //                         logger.info('queryDirectInvestPath_totalPathNum: ' + totalPathNum);
    //                         return reply.response({ code: code, results: res.pathDetail });
    //                     }
    //                 }).catch(err => {
    //                     return reply.response({ ok: -1, message: err.message || err });
    //                 });
    //         } else if (!code) {
    //             return reply.response(errorResp(errorCode.ARG_ERROR, `缺少code参数!`));
    //         }

    //     } catch (err) {
    //         return reply.response(err);
    //     }
    // },

    //担保关系路径
    queryGuaranteePathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let GTDepth = request.query.guaranteePathDepth;
        if (!GTDepth) GTDepth = config.pathDepth.GTDepth;
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryGuaranteePath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryGuaranteePath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryGuaranteePath(codeOne, codeTwo, GTDepth)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryGuaranteePath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryGuaranteePath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: "no results!" });
                            }
                            else if (res) {
                                let totalPathNum = res.pathDetail.data.pathNum;
                                console.log(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteePath_totalPathNum: ' + totalPathNum);
                                logger.info(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteePath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: res.pathDetail });
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //被担保关系路径
    queryGuaranteedByPathInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let GTBDepth = request.query.guaranteedByPathDepth;
        if (!GTBDepth) GTBDepth = config.pathDepth.GTBDepth;
        let user = request.query.username;                                 //调用接口的用户信息
        if (!user) {
            user = 'unknown';
        }
        try {
            let res = null;
            if (codeOne && codeTwo) {
                if (codeOne == codeTwo) {
                    console.error('from can not be same as to!');
                    logger.error('from can not be same as to!');
                    return reply.response(errorResp(errorCode.ARG_ERROR, 'from/to参数不能相同!'));
                }
                else if (codeOne != codeTwo) {
                    let now = Date.now();
                    console.log('user: ' +user +', queryGuaranteeedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    logger.info('user: ' +user +', queryGuaranteedByPath  from: ' + codeOne + ', to: ' + codeTwo);
                    searchGraph.queryGuaranteedByPath(codeOne, codeTwo, GTBDepth)
                        .then(res => {
                            let totalQueryCost = Date.now() - now;
                            logger.info(`user: ${user}, from: ${codeOne} to: ${codeTwo}` + " queryGuaranteeedByPath_totalQueryCost: " + totalQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` user: ${user}, from: ${codeOne} to: ${codeTwo}` + ", queryGuaranteeedByPath_totalQueryCost: " + totalQueryCost + 'ms');

                            if (!res) {
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: "no results!" });
                            }
                            else if (res) {
                                let totalPathNum = res.pathDetail.data.pathNum;
                                console.log(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteeedByPath_totalPathNum: ' + totalPathNum);
                                logger.info(`from: ${codeOne} to: ${codeTwo}` + ' queryGuaranteeedByPath_totalPathNum: ' + totalPathNum);
                                return reply.response({ direction: { from: codeOne, to: codeTwo }, results: res.pathDetail });
                            }
                        }).catch(err => {
                            return reply.response({ ok: -1, message: err.message || err });
                        });
                }
            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }

        } catch (err) {
            return reply.response(err);
        }
    },

    //添加需要预加热的查询条件信息
    addWarmUpQueryDataInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let IVDepth = parseInt(request.query.investPathDepth);                            //直接投资路径查询深度
        if (!IVDepth) IVDepth = config.pathDepth.IVDepth;
        let IVBDepth = parseInt(request.query.investedByPathDepth);                       //直接被投资路径查询深度
        if (!IVBDepth) IVBDepth = config.pathDepth.IVBDepth;
        let CIVDepth = parseInt(request.query.comInvestPathDepth);                        //共同投资关系路径查询深度
        if (!CIVDepth) CIVDepth = config.pathDepth.CIVDepth;
        let CIVBDepth = parseInt(request.query.comInvestedByPathDepth);                   //共同被投资关系路径查询深度
        if (!CIVBDepth) CIVBDepth = config.pathDepth.CIVBDepth;
        try {
            if (codeOne && codeTwo) {
                let now = Date.now();
                console.log('warmUpData  from: ' + codeOne + ', to: ' + codeTwo);
                logger.info('warmUpData  from: ' + codeOne + ', to: ' + codeTwo);
                //1、先将符合预热条件的数据存到redis中
                let conditionsKey = config.redisKeyName.conditionsKey;
                let conditionsField = [codeOne, codeTwo].join('->');
                let conditionsValue = { from: codeOne, to: codeTwo, depth: [IVDepth, IVBDepth, CIVDepth, CIVBDepth], relations: [0, 1, 4, 5] };
                cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                //2、再将先前符合预热条件的conditionsValue作为key去筛选path, path作为value存入redis
                for (let subRelation of conditionsValue.relations) {
                    if (subRelation == 0) {
                        // let warmUpKey = [conditionsValue.from, conditionsValue.to, conditionsValue.depth[0], subRelation].join('-');
                        await searchGraph.queryInvestPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[0], 0, 100);
                    }
                    else if (subRelation == 1) {
                        await searchGraph.queryInvestedByPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[1], 0, 100);
                    }
                    // else if (subRelation == 3) {
                    //     searchGraph.queryfullPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[2], 0, 100);
                    // }
                    else if (subRelation == 4) {
                        await searchGraph.queryCommonInvestPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[2], 0, 100);
                    }
                    else if (subRelation == 5) {
                        await searchGraph.queryCommonInvestedByPath(conditionsValue.from, conditionsValue.to, conditionsValue.depth[3], 0, 100);
                    }
                }
                return reply.response({ addWarmUpQueryDataInfo: 'ok' });

            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //删除预加热的查询条件信息
    deleteWarmUpQueryDataInfo: async function (request, reply) {
        let codeOne = request.query.from;
        let codeTwo = request.query.to;
        let IVDepth = parseInt(request.query.investPathDepth);                            //直接投资路径查询深度
        if (!IVDepth) IVDepth = config.pathDepth.IVDepth;
        let IVBDepth = parseInt(request.query.investedByPathDepth);                       //直接被投资路径查询深度
        if (!IVBDepth) IVBDepth = config.pathDepth.IVBDepth;
        let CIVDepth = parseInt(request.query.comInvestPathDepth);                        //共同投资关系路径查询深度
        if (!CIVDepth) CIVDepth = config.pathDepth.CIVDepth;
        let CIVBDepth = parseInt(request.query.comInvestedByPathDepth);                   //共同被投资关系路径查询深度
        if (!CIVBDepth) CIVBDepth = config.pathDepth.CIVBDepth;
        try {
            if (codeOne && codeTwo) {
                let now = Date.now();
                console.log('deleteWarmUpData  from: ' + codeOne + ', to: ' + codeTwo);
                logger.info('deleteWarmUpData  from: ' + codeOne + ', to: ' + codeTwo);
                //1、先从redis中获取符合预热条件的数据
                let conditionsKey = config.redisKeyName.conditionsKey;
                let conditionsField = [codeOne, codeTwo].join('->');
                let conditionsValue = await cacheHandlers.getWarmUpConditionsFromRedis(conditionsKey, conditionsField);
                if (!conditionsValue) {
                    console.log('the WarmUpConditions is not in the redis, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    logger.info('the WarmUpConditions is not in the redis, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    return reply.response({ deleteWarmUpData: 'the WarmUpConditions is not exist!' });
                }
                else if (conditionsValue) {
                    cacheHandlers.deleteWarmUpConditionsField(conditionsKey, conditionsField);
                    console.log('the WarmUpConditions delete info, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    logger.info('the WarmUpConditions delete info, the conditionsKey is: ' + conditionsKey + ', the conditionsField is: ' + conditionsField);
                    return reply.response({ deleteWarmUpData: 'ok' });
                }

            } else if (!codeOne && codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from参数!`));
            } else if (codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少to参数!`));
            } else if (!codeOne && !codeTwo) {
                return reply.response(errorResp(errorCode.ARG_ERROR, `缺少from和to参数!`));
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //查询所有的预热数据的key对应的field
    listWarmUpConditionsFieldInfo: async function (request, reply) {
        try {
            let conditionsKey = request.query.conditionsKey;
            if (!conditionsKey) conditionsKey = config.redisKeyName.conditionsKey;
            let conditionsField = await cacheHandlers.findWarmUpConditionsField(conditionsKey);
            if (!conditionsField) {
                return reply.response({ conditionsKey: conditionsKey, conditionsField: [], fieldsNum: 0 });
            }
            else if (conditionsField) {
                return reply.response({ conditionsKey: conditionsKey, conditionsField: conditionsField, fieldsNum: conditionsField.length });
            }
        } catch (err) {
            return reply.response(err);
        }
    },

    //外部触发接口主动查询需要预热的path数据
    startWarmUpPaths: async function (request, reply) {
        try {
            redlock.lock(lockResource, lockTTL).then(async function (lock) {
                timingWarmUpPaths('true');
            });
            return reply.response({ ok: 1, info: 'start warming up paths...' });
        } catch (err) {
            return reply.response(err);
        }
    },

    //删除lockResource
    deleteLockResource: async function (request, reply) {
        try {
            let lockResource = request.query.lockResource;
            if (!lockResource) {
                lockResource = config.lockInfo.resource[0];
            }
            redis.del(lockResource);
            redis1.del(lockResource);
            return reply.response({ ok: 1, message: `delete the lock resource: '${lockResource}' succeess!` })

        } catch (err) {
            return reply.response(err);
        }
    },

    //删除预热的paths数据
    deleteWarmUpPaths: async function (request, reply) {
        try {
            cacheHandlers.deleteWarmUpPathsFromRedis();
            return reply.response({ ok: 1, message: 'delete the warmup paths succeess!' });
        } catch (err) {
            console.error(err);
            logger.error(err);
            return reply.response(err);
        }
    }

}


module.exports = apiHandlers;