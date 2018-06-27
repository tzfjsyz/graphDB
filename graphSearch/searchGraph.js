/*
用于企业关联关系信息查询
wrote by tzf, 2017/12/8
*/
const cacheHandlers = require('./cacheHandlers.js');
const req = require('require-yml')
const config = req("config/source.yml");
const Hapi = require('hapi');
const server = new Hapi.Server();
const moment = require('moment');
const log4js = require('log4js');
// const Promise = require('bluebird');
const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver(`${config.neo4jServer.url}`, neo4j.auth.basic(`${config.neo4jServer.user}`, `${config.neo4jServer.password}`),
    {
        maxConnectionLifetime: 30 * 60 * 60,
        maxConnectionPoolSize: 1000,
        connectionAcquisitionTimeout: 2 * 60
    }
);
// const lookupTimeout = config.lookupTimeout;
// console.log('lookupTimeout: ' + lookupTimeout + 'ms');

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

//数组元素去重
function unique(arr) {
    var result = [], hash = {};
    for (var i = 0, elem; (elem = arr[i]) != null; i++) {
        if (!hash[elem]) {
            result.push(elem);
            hash[elem] = true;
        }
    }
    return result;
}

//路径数组元素去除出现两次以上的路径
function uniquePath(pathes, codes) {
    let uniqueFlag = true;
    for (let subCode of codes) {
        let codeIndex = 0;
        for (let subPath of pathes) {
            if (subPath.sourceCode == subCode || subPath.targetCode == subCode) codeIndex++;
        }
        if (codeIndex >= 3) {
            uniqueFlag = false;
            return uniqueFlag;
        }
    }
    return uniqueFlag;
}

function findNodeId(code) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        let resultPromise = session.run(`match (compId:company {ITCode2: ${code}}) return compId`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length == 0)
                return resolve(null);
            let id = result.records[0]._fields[0].identity.low;
            // driver.close();
            if (id)
                return resolve(id);
            else
                return resolve(0);
        }).catch(err => {
            console.error(err);
            logger.error(err);
            // session.close();
            // driver.close();
            return reject(err);
        });
    });
}

//获取outbound的节点数
function getOutBoundNodeNum(code, depth) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        // let resultPromise = session.run(`start from=node(${nodeId}) match (from)-[r:invests*..${depth-1}]->(to) return count(to)`);
        // let resultPromise = session.run(`start from=node(${nodeId}) match (from)-[r:invests*..1]->(to) return count(to)`);
        let resultPromise = session.run(`match (from:company{ITCode2: ${code}})-[r:invests*..1]->(to) return count(to)`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length == 0)
                return resolve(null);
            let count = result.records[0]._fields[0].low;
            // driver.close();
            if (count)
                return resolve(count);
            else
                return resolve(0);
        }).catch(err => {
            console.error(err);
            logger.error(err);
            // session.close();
            // driver.close();
            return reject(err);
        });
    });
}

//获取outbound的节点数
function getInBoundNodeNum(code, depth) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        // let resultPromise = session.run(`start from=node(${nodeId}) match (from)<-[r:invests*..${depth-1}]-(to) return count(to)`);
        // let resultPromise = session.run(`start from=node(${nodeId}) match (from)<-[r:invests*..1]-(to) return count(to)`);
        let resultPromise = session.run(`match (from:company{ITCode2: ${code}})<-[r:invests*..1]-(to) return count(to)`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length == 0)
                return resolve(null);
            let count = result.records[0]._fields[0].low;
            // driver.close();
            if (count)
                return resolve(count);
            else
                return resolve(0);
        }).catch(err => {
            console.error(err);
            logger.error(err);
            // session.close();
            // driver.close();
            return reject(err);
        });
    });
}

//获取担保关系的outbound的节点数
function getGuaranteeOutBoundNodeNum(code, depth) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        let resultPromise = session.run(`match (from:company{ITCode2: ${code}})-[r:guarantees*..1]->(to) return count(to)`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length == 0)
                return resolve(null);
            let count = result.records[0]._fields[0].low;
            // driver.close();
            if (count)
                return resolve(count);
            else
                return resolve(0);
        }).catch(err => {
            console.error(err);
            logger.error(err);
            // session.close();
            // driver.close();
            return reject(err);
        });
    });
}

//获取担保关系的outbound的节点数
function getGuaranteeInBoundNodeNum(code, depth) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        let resultPromise = session.run(`match (from:company{ITCode2: ${code}})<-[r:guarantees*..1]-(to) return count(to)`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length == 0)
                return resolve(null);
            let count = result.records[0]._fields[0].low;
            // driver.close();
            if (count)
                return resolve(count);
            else
                return resolve(0);
        }).catch(err => {
            console.error(err);
            logger.error(err);
            // session.close();
            // driver.close();
            return reject(err);
        });
    });
}

//处理ITCode2查询ITName为空的情况
function handlerAllNames(names) {
    let newNames = new Set();
    for (let subName of names) {
        if (subName != '') newNames.add(subName);
        else if (subName == '') {
            subName = 'ITName is null!';
            newNames.add(subName);
        }
    }
    return Array.from(newNames);
}

//将ITCode2->ITName对放入Map中
function getCodeNameMapping(codes, names) {
    let codeNameMap = new Map();
    if (codes.length == names.length) {
        for (let i = 0; i < codes.length; i++) {
            codeNameMap.set(codes[i], names[i]);
        }
        return codeNameMap;
    }
}

/* 质朴长存法, num：原数字, n：最终生成的数字位数*/
function pad(num, n) {
    var len = num.toString().length;
    while (len < n) {
        num = "0" + num;
        len++;
    }
    return num;
}

//判断重复的path
function isDuplicatedPath(pathGroup, pathDetail) {
    let flag = false;
    let groupLen = pathGroup.length;
    let breakTimes = 0;
    if (groupLen != 0) {
        for (let i = 0; i < groupLen; i++) {
            let groupPathLen = pathGroup[i].path.length;
            let pathDetailLen = pathDetail.path.length;
            if (groupPathLen == pathDetailLen) {
                for (let j = 0; j < groupPathLen; j++) {
                    let path_one = pathGroup[i].path[j];
                    let path_two = pathDetail.path[j];
                    if (path_one.sourceCode != path_two.sourceCode || path_one.targetCode != path_two.targetCode) {
                        breakTimes++;
                        break;
                    }
                }
            }
            else {
                breakTimes++;
            }
        }
        console.log('groupLen: ' + groupLen + ', breakTimes: ' + breakTimes + '次');
        if (breakTimes != groupLen) {
            flag = true;                                                //eachPath的sourceCode 和 targetCode与pathArray中path有一致的存在，即重复的path 
            console.log('过滤1条重复path！');
        }
    }
    return flag;
}

//判断每个path下是否存在source/target出现2次以上的情况(source >=2: 共同被投资; target >=2: 共同被投资)
function findCodeAppearTimes(pathDetail) {
    let sourceArray = [];
    let targetArray = [];
    let sourceIsRepeat = false;
    let targetIsRepeat = false;
    let result = {};
    for (let subPathDetail of pathDetail) {
        let sourceCode = subPathDetail.sourceCode;
        let targetCode = subPathDetail.targetCode;
        sourceArray.push(sourceCode);
        targetArray.push(targetCode);
    }
    result.sourceIsRepeat = isRepeat(sourceArray);
    result.targetIsRepeat = isRepeat(targetArray);
    return result;
}

//数组是否存在重复元素
function isRepeat(array) {
    let hash = {};
    for (let i in array) {
        if (hash[array[i]]) {
            return true;
        }
        hash[array[i]] = true;
    }
    return false;
}


//判断invest path的source/target连接是否有断点, 即是否有guarantees关系连接才形成的path
function isContinuousPath(from, to, pathArray) {
    from = parseInt(from);
    to = parseInt(to);
    let flag = true;
    let pathLength = pathArray.length;
    if (pathLength > 1) {
        for (let i = 0; i < pathLength - 1; i++) {
            let nextSourceCode = pathArray[i + 1].sourceCode;
            let targetCode = pathArray[i].targetCode;
            let sourceCode = pathArray[i].sourceCode;
            let nextTargetCode = pathArray[i + 1].targetCode;
            if (targetCode != nextSourceCode || sourceCode == nextTargetCode || targetCode == from || targetCode == to) {                     //targetCode != nextSourceCode:不连续的path;  sourceCode == nextTargetCode：闭环的path
                console.log('该invest path不符合要求！');
                flag = false;
                break;
            }
        }
    }
    return flag;
}

//判断invest path的是否闭环
function isClosedLoopPath(from, to, pathArray) {
    from = parseInt(from);
    to = parseInt(to);
    let flag = false;
    let pathLength = pathArray.length;
    if (pathLength > 1) {
        for (let i = 0; i < pathLength; i++) {
            let sourceCode = pathArray[i].sourceCode;
            let targetCode = pathArray[i].targetCode;
            if ((sourceCode == from && targetCode == to) || (sourceCode == to && targetCode == from)) {
                console.log('该invest path不符合要求！');
                flag = true;
                break;
            }
        }
    }
    return flag;
}

//处理neo4j返回的JSON数据格式
async function handlerPromise(from, to, result, index) {
    let allNamesOne = [];
    let allCodesOne = new Set();                                    //使用set保证数据的唯一性
    let newAllCodesOne = new Set();
    let uniqueCodesOne = [];                                            //存储唯一性的codes数据元素
    // let num = 0;

    let allNamesTwo = [];
    let allCodesTwo = new Set();                                    //使用set保证数据的唯一性
    let uniqueCodesTwo = [];                                            //存储唯一性的codes数据元素

    let pathTypeName = "";
    if (index == 0)
        pathTypeName = "InvestPath";
    else if (index == 1)
        pathTypeName = "InvestedByPath";
    else if (index == 2)
        pathTypeName = "ShortestPath";
    else if (index == 3)
        pathTypeName = "FullPath";
    //只有在单独调用接口时会使用到                                     //记录每种路径查询方式下的具体路径数
    else if (index == 4)
        pathTypeName = "CommonInvestPath";
    else if (index == 5)
        pathTypeName = "CommonInvestedByPath";
    else if (index == 6)
        pathTypeName = "DirectInvestPath";
    else if (index == 7)
        pathTypeName = "DirectInvestedByPath";
    else if (index == 8)
        pathTypeName = "GuaranteePath";
    else if (index == 9)
        pathTypeName = "GuaranteedByPath";
    let promiseResult = { pathTypeOne: {}, pathTypeTwo: {} };
    let promiseResultOne = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${index}`, typeName: pathTypeName, names: [] } };
    let promiseResultTwo = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [] } };

    // promiseResult.dataDetail.data.pathNum = result.records.length;
    // num += result.records.length;                                  //调用 neo4j 返回的所有路径，包括有重复点的路径

    if (result.records.length == 0) {
        promiseResult.pathTypeOne = promiseResultOne;
        promiseResult.pathTypeTwo = promiseResultTwo;
        return promiseResult;
    }
    else if (result.records.length > 0) {
        let tempResultTwo = [];                                       //临时存放所有的担保关系路径
        let newTempResultTwo = [];                                    //筛选后的担保关系路径
        for (let subRecord of result.records) {
            let pathArrayOne = {};
            let uniquePathArrayOne = {};
            let eachPathArrayOne = [];
            let tempPathArrayOne = [];
            // let tempPathSetOne = new Set();

            let pathArrayTwo = {};
            let uniquePathArrayTwo = {};
            let eachPathArrayTwo = [];
            let tempPathArrayTwo = [];
            // let tempPathSetTwo = new Set();
            let pathOneCodeSet = new Set();                                                              //存放pathOne中所有的ITCode2

            for (let subField of subRecord._fields) {

                for (let subSegment of subField.segments) {

                    //取到start nodes的isPerson属性
                    let startIsPerson = null;
                    if (subSegment.start.properties.isPerson) {
                        startIsPerson = subSegment.start.properties.isPerson;
                    }
                    else if (!subSegment.start.properties.isPerson && subSegment.start.properties.isPerson.low) {
                        startIsPerson = subSegment.start.properties.isPerson.low;
                    }

                    //取到end nodes的isPerson属性
                    let endIsPerson = null;
                    if (subSegment.end.properties.isPerson) {
                        endIsPerson = subSegment.end.properties.isPerson;
                    }
                    else if (!subSegment.end.properties.isPerson && subSegment.end.properties.isPerson.low) {
                        endIsPerson = subSegment.end.properties.isPerson.low;
                    }

                    //通过isPerson为ITCode2取值
                    let startSegmentCode = null;
                    let endSegmentCode = null;
                    if (startIsPerson == '1' || startIsPerson == 1) {
                        let id = subSegment.start.properties.ITCode2.low;
                        startSegmentCode = 'P' + pad(id, 9);
                    } else {
                        startSegmentCode = subSegment.start.properties.ITCode2.low;
                    }
                    if (endIsPerson == '1' || endIsPerson == 1) {
                        let id = subSegment.end.properties.ITCode2.low;
                        endSegmentCode = 'P' + pad(id, 9);
                    } else {
                        endSegmentCode = subSegment.end.properties.ITCode2.low;
                    }

                    // allNames.push(subSegment.start.properties.ITName, subSegment.end.properties.ITName);
                    // allCodes.push(subSegment.start.properties.ITCode2, subSegment.end.properties.ITCode2);
                    let startSegmentLow = subSegment.start.identity.low;
                    // let startSegmentName = subSegment.start.properties.ITName;
                    // let startSegmentCode = subSegment.start.properties.ITCode2.low;
                    let relSegmentStartLow = subSegment.relationship.start.low;
                    // let startSegmentCode = subSegment.start.properties.ITCode2;
                    // let relSegmentStartLow = subSegment.relationship.start.low;
                    let startRegFund = 0;                                                              //注册资金
                    if (subSegment.start.properties.regFund) {
                        startRegFund = subSegment.start.properties.regFund;
                        startRegFund = parseFloat(startRegFund.toFixed(2));                            //将RegFund值转换2位小数              
                    }
                    else if (!subSegment.start.properties.regFund && subSegment.start.properties.regFund.low) {
                        startRegFund = subSegment.start.properties.regFund.low;
                        startRegFund = parseFloat(startRegFund.toFixed(2));                            //将RegFund值转换2位小数                           
                    }

                    let relSegmentEndLow = subSegment.relationship.end.low;
                    //如果weight是全量导入的，直接取weight的值；如果weight是增量导入的，需要取到weight节点下的low值
                    let weight = 0;                                                                    //持股比例
                    if (Object.keys(subSegment.relationship.properties).length > 0) {
                        if (subSegment.relationship.properties.weight) {
                            weight = subSegment.relationship.properties.weight;
                            weight = parseFloat(weight.toFixed(2));                                        //将weight值转换2位小数
                        }
                        else if (!subSegment.relationship.properties.weight && subSegment.relationship.properties.weight.low) {
                            weight = subSegment.relationship.properties.weight.low;
                            weight = parseFloat(weight.toFixed(2));                                        //将weight值转换2位小数
                        }
                    }

                    //获取每个path relationship type
                    let relationshipType = null;
                    if (Object.keys(subSegment.relationship.type).length > 0) {
                        relationshipType = subSegment.relationship.type;
                    }

                    let endSegmentLow = subSegment.end.identity.low;
                    // let endSegmentName = subSegment.end.properties.ITName;
                    // let endSegmentCode = subSegment.end.properties.ITCode2.low;
                    // let endSegmentCode = subSegment.end.properties.ITCode2;
                    let endRegFund = 0;                                                               //注册资金
                    if (subSegment.end.properties.regFund) {
                        endRegFund = subSegment.end.properties.regFund;
                        endRegFund = parseFloat(endRegFund.toFixed(2));                               //将RegFund值转换2位小数                        
                    }
                    else if (!subSegment.end.properties.regFund && subSegment.end.properties.regFund.low) {
                        endRegFund = subSegment.start.properties.regFund.low;
                        endRegFund = parseFloat(endRegFund.toFixed(2));                               //将RegFund值转换2位小数                           
                    }
                    // let startSegmentName = await queryCodeToName(startSegmentCode);                 //不直接获取eno4j中的ITName，而是通过外部接口由ITCode2获取ITName
                    // let endSegmentName = await queryCodeToName(endSegmentCode);
                    // allNames.push(startSegmentName, endSegmentName);

                    // allCodes.push(`${startSegmentCode}`, `${endSegmentCode}`);

                    //处理无机构代码的start nodes的name问题
                    let startSegmentName = null;
                    if (subSegment.start.properties.isExtra) {
                        if (subSegment.start.properties.isExtra == 1 || subSegment.start.properties.isExtra == '1' || subSegment.start.properties.isExtra == 'null') {
                            startSegmentName = subSegment.start.properties.name;
                        } else if (subSegment.start.properties.isExtra == 0 || subSegment.start.properties.isExtra == '0') {
                            startSegmentName = null;
                            // allCodes.push(`${startSegmentCode}`);  
                            // allCodes.add(`${startSegmentCode}`);                                                                        //将有机构代码的ITCode存入数组

                            // if (relationshipType == 'invests' || !relationshipType) {
                            //     allCodesOne.add(`${startSegmentCode}`);
                            //     // subFieldCodesOne.add(endSegmentCode);   
                            // }
                            // if (relationshipType == 'guarantees') {
                            //     allCodesTwo.add(`${startSegmentCode}`);
                            // subFieldCodesTwo.add(endSegmentCode);
                            // }
                        }
                    } else if (!subSegment.start.properties.isExtra && subSegment.start.properties.isExtra.low) {
                        if (subSegment.start.properties.isExtra.low == 1 || subSegment.start.properties.isExtra.low == '1' || subSegment.start.properties.isExtra == 'null') {
                            startSegmentName = subSegment.start.properties.name;
                        } else if (subSegment.start.properties.isExtra.low == 0 || subSegment.start.properties.isExtra.low == '0') {
                            startSegmentName = null;
                            // allCodes.push(`${startSegmentCode}`);                                                               //将有机构代码的ITCode存入数组
                            // allCodes.add(`${startSegmentCode}`);

                            // if (relationshipType == 'invests' || !relationshipType) {
                            //     allCodesOne.add(`${startSegmentCode}`);
                            //     // subFieldCodesOne.add(endSegmentCode);   
                            // }
                            // if (relationshipType == 'guarantees') {
                            //     allCodesTwo.add(`${startSegmentCode}`);
                            // subFieldCodesTwo.add(endSegmentCode);
                            // }
                        }
                    }

                    //处理无机构代码的end nodes的name问题
                    let endSegmentName = null;
                    if (subSegment.end.properties.isExtra) {
                        if (subSegment.end.properties.isExtra == 1 || subSegment.end.properties.isExtra == '1' || subSegment.end.properties.isExtra == 'null') {
                            endSegmentName = subSegment.end.properties.name;
                        } else if (subSegment.end.properties.isExtra == 0 || subSegment.end.properties.isExtra == '0') {
                            endSegmentName = null;
                            // allCodes.push(`${endSegmentCode}`);                                                                   //将有机构代码的ITCode存入数组
                            // allCodes.add(`${endSegmentCode}`);

                            // if (relationshipType == 'invests' || !relationshipType) {
                            //     // subFieldCodesOne.add(startSegmentCode);
                            //     allCodesOne.add(`${endSegmentCode}`);
                            // }
                            // if (relationshipType == 'guarantees') {
                            // subFieldCodesTwo.add(startSegmentCode);
                            //     allCodesTwo.add(`${endSegmentCode}`);
                            // }
                        }
                    } else if (!subSegment.end.properties.isExtra && subSegment.end.properties.isExtra.low) {
                        if (subSegment.end.properties.isExtra.low == 1 || subSegment.end.properties.isExtra.low == '1' || subSegment.end.properties.isExtra == 'null') {
                            endSegmentName = subSegment.end.properties.name;
                        } else if (subSegment.end.properties.isExtra.low == 0 || subSegment.end.properties.isExtra.low == '0') {
                            endSegmentName = null;
                            // allCodes.push(`${endSegmentCode}`);                                                                   //将有机构代码的ITCode存入数组
                            // allCodes.add(`${endSegmentCode}`);

                            // if (relationshipType == 'invests' || !relationshipType) {
                            //     // subFieldCodesOne.add(startSegmentCode);
                            //     allCodesOne.add(`${endSegmentCode}`);
                            // }
                            // if (relationshipType == 'guarantees') {
                            // subFieldCodesTwo.add(startSegmentCode);
                            // allCodesTwo.add(`${endSegmentCode}`);
                            // }
                        }
                    }

                    //取到start nodes的isExtra属性
                    let startIsExtra = null;
                    if (subSegment.start.properties.isExtra) {
                        startIsExtra = subSegment.start.properties.isExtra;
                    } else if (!subSegment.start.properties.isExtra && subSegment.start.properties.isExtra.low) {
                        startIsExtra = subSegment.start.properties.isExtra.low;
                    }

                    //取到end nodes的isExtra属性
                    let endIsExtra = null;
                    if (subSegment.end.properties.isExtra) {
                        endIsExtra = subSegment.end.properties.isExtra;
                    } else if (!subSegment.end.properties.isExtra && subSegment.end.properties.isExtra.low) {
                        endIsExtra = subSegment.end.properties.isExtra.low;
                    }

                    let pathOne = {                                                                               //invests的path
                        source: null, sourceCode: null, sourceRegFund: null, sourceIsExtra: null, sourceIsPerson: null,
                        target: null, targetCode: null, targetRegFund: null, targetIsExtra: null, targetIsPerson: null, weight: null,
                    };
                    let pathTwo = {                                                                               //guarantees的path
                        source: null, sourceCode: null, sourceRegFund: null, sourceIsExtra: null, sourceIsPerson: null,
                        target: null, targetCode: null, targetRegFund: null, targetIsExtra: null, targetIsPerson: null, weight: null,
                    };

                    if (relationshipType == 'invests' || !relationshipType) {
                        if (relSegmentStartLow == startSegmentLow && relSegmentEndLow == endSegmentLow) {
                            pathOne.source = startSegmentName;
                            pathOne.sourceCode = startSegmentCode;
                            pathOne.sourceRegFund = startRegFund;
                            pathOne.sourceIsExtra = startIsExtra;
                            pathOne.sourceIsPerson = startIsPerson;
                            pathOne.target = endSegmentName;
                            pathOne.targetCode = endSegmentCode;
                            pathOne.targetRegFund = endRegFund;
                            pathOne.targetIsExtra = endIsExtra;
                            pathOne.targetIsPerson = endIsPerson;
                            pathOne.weight = weight;
                            // pathOne.pathType = relationshipType;

                            //如果isPerson = true 过滤掉
                            if (startIsPerson != 1 || startIsPerson != '1') {
                                pathOneCodeSet.add(`${startSegmentCode}`);
                            }
                            if (endIsPerson != 1 || endIsPerson != '1') {
                                pathOneCodeSet.add(`${endSegmentCode}`);
                            }
                        }
                        else if (relSegmentStartLow == endSegmentLow && relSegmentEndLow == startSegmentLow) {
                            pathOne.source = endSegmentName;
                            pathOne.sourceCode = endSegmentCode;
                            pathOne.sourceRegFund = endRegFund;
                            pathOne.sourceIsExtra = endIsExtra;
                            pathOne.sourceIsPerson = endIsPerson;
                            pathOne.target = startSegmentName;
                            pathOne.targetCode = startSegmentCode;
                            pathOne.targetRegFund = startRegFund;
                            pathOne.targetIsExtra = startIsExtra;
                            pathOne.targetIsPerson = startIsPerson;
                            pathOne.weight = weight;
                            // pathOne.pathType = relationshipType;

                            //如果isPerson = true 过滤掉
                            if (startIsPerson != 1 || startIsPerson != '1') {
                                pathOneCodeSet.add(`${startSegmentCode}`);
                            }
                            if (endIsPerson != 1 || endIsPerson != '1') {
                                pathOneCodeSet.add(`${endSegmentCode}`);
                            }

                        }
                    }
                    else if (relationshipType == 'guarantees') {
                        if (relSegmentStartLow == startSegmentLow && relSegmentEndLow == endSegmentLow) {
                            pathTwo.source = startSegmentName;
                            pathTwo.sourceCode = startSegmentCode;
                            pathTwo.sourceRegFund = startRegFund;
                            pathTwo.sourceIsExtra = startIsExtra;
                            pathTwo.sourceIsPerson = startIsPerson;
                            pathTwo.target = endSegmentName;
                            pathTwo.targetCode = endSegmentCode;
                            pathTwo.targetRegFund = endRegFund;
                            pathTwo.targetIsExtra = endIsExtra;
                            pathTwo.targetIsPerson = endIsPerson;
                            pathTwo.weight = weight;
                            // pathTwo.pathType = relationshipType;

                        }
                        else if (relSegmentStartLow == endSegmentLow && relSegmentEndLow == startSegmentLow) {
                            pathTwo.source = endSegmentName;
                            pathTwo.sourceCode = endSegmentCode;
                            pathTwo.sourceRegFund = endRegFund;
                            pathTwo.sourceIsExtra = endIsExtra;
                            pathTwo.sourceIsPerson = endIsPerson;
                            pathTwo.target = startSegmentName;
                            pathTwo.targetCode = startSegmentCode;
                            pathTwo.targetRegFund = startRegFund;
                            pathTwo.targetIsExtra = startIsExtra;
                            pathTwo.targetIsPerson = startIsPerson;
                            pathTwo.weight = weight;
                            // pathTwo.pathType = relationshipType;

                        }
                    }

                    if (pathOne.sourceCode != null && pathOne.targetCode != null) {
                        // delete pathOne.pathType;                                             //删除pathType属性
                        tempPathArrayOne.push(pathOne);
                    }
                    //invests关系存在的前提下, 添加guarantees关系的path
                    if (pathTwo.sourceCode != null && pathTwo.targetCode != null && pathTwo.sourceCode != pathTwo.targetCode) {
                        // delete pathTwo.pathType;
                        tempPathArrayTwo.push(pathTwo);
                    }
                }

                //除全部路径，即双向关系时，其他路径需要过滤重复的path
                // if (index == 3) {
                //     uniquePathArray = pathArray;
                // }
                // else if (index != 3) {
                //     let isUniquePath = uniquePath(pathArray.path, uniqueFieldCodes);
                //     if (isUniquePath) uniquePathArray = pathArray;
                //     if (!isUniquePath) {
                //         // pathNum--;
                //         num--;
                //     }
                // }

            }
            //判断form/to中是否有自然人
            if (from.toString().length != 10 || to.toString().length != 10) {
                pathArrayOne.path = tempPathArrayOne;
            }
            else if (from.toString().length == 10 && to.toString().length == 10) {
                //如果from和to任何一个不在pathOne中，则剔除这条path
                if (pathOneCodeSet.has(from) && pathOneCodeSet.has(to)) {
                    pathArrayOne.path = tempPathArrayOne;
                    //将每个path下的pathOneCodeSet中的元素添加到allCodesOne中
                    allCodesOne = new Set([...allCodesOne, ...pathOneCodeSet]);
                }
            }

            pathArrayTwo.path = tempPathArrayTwo;

            //处理投资关系--pathArrayOne
            if (pathArrayOne.hasOwnProperty('path') && pathArrayOne.path.length > 0) {
                // promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                //过滤重复的path
                let pathGroup = promiseResultOne.dataDetail.data.pathDetail;
                let isDupFlag = isDuplicatedPath(pathGroup, pathArrayOne);
                if (isDupFlag == false) {

                    //判断form/to都不是自然人
                    if (from.toString().length == 10 && to.toString().length == 10) {
                        //处理共同投资和共同股东关系路径
                        if (index == 4 || index == 5) {
                            let sourceCodeVal = pathArrayOne.path[0].sourceCode;
                            let targetCodeVal = pathArrayOne.path[0].targetCode;
                            let fromCode = parseInt(from);
                            let toCode = parseInt(to);
                            let result = findCodeAppearTimes(pathArrayOne.path);
                            //判断path是否闭环
                            let isClosedLoopFlag = isClosedLoopPath(from, to, pathArrayOne.path);
                            if (isClosedLoopFlag == false && pathArrayOne.path.length > 1 && (result.sourceIsRepeat == true || result.targetIsRepeat == true)) {
                                //处理共同投资关系路径
                                if (index == 4 && result.targetIsRepeat == true) {
                                    if ((sourceCodeVal == fromCode && targetCodeVal == toCode) || (sourceCodeVal == toCode && targetCodeVal == fromCode)) {
                                        console.log('delete the path !');
                                    }
                                    else {
                                        promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                        for (let subPath of pathArrayOne.path) {
                                            let isPerson_source = subPath.sourceIsPerson;
                                            let isPerson_target = subPath.targetIsPerson;
                                            let ITCode_source = subPath.sourceCode;
                                            let ITCode_target = subPath.targetCode;
                                            if (isPerson_source != 1 && isPerson_source != '1') {
                                                newAllCodesOne.add(`${ITCode_source}`);
                                            }
                                            if (isPerson_target != 1 && isPerson_target != '1') {
                                                newAllCodesOne.add(`${ITCode_target}`);
                                            }
                                        }
                                    }
                                }
                                //处理共同股东关系路径
                                if (index == 5 && result.sourceIsRepeat == true) {
                                    if ((sourceCodeVal == fromCode && targetCodeVal == toCode) || (sourceCodeVal == toCode && targetCodeVal == fromCode)) {
                                        console.log('delete the path !');
                                    }
                                    else {
                                        promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                        for (let subPath of pathArrayOne.path) {
                                            let isPerson_source = subPath.sourceIsPerson;
                                            let isPerson_target = subPath.targetIsPerson;
                                            let ITCode_source = subPath.sourceCode;
                                            let ITCode_target = subPath.targetCode;
                                            if (isPerson_source != 1 && isPerson_source != '1') {
                                                newAllCodesOne.add(`${ITCode_source}`);
                                            }
                                            if (isPerson_target != 1 && isPerson_target != '1') {
                                                newAllCodesOne.add(`${ITCode_target}`);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        //处理直接投资关系路径
                        else if (index == 0) {
                            let flag = isContinuousPath(from, to, pathArrayOne.path);
                            if (flag == true) {
                                promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                for (let subPath of pathArrayOne.path) {
                                    let isPerson_source = subPath.sourceIsPerson;
                                    let isPerson_target = subPath.targetIsPerson;
                                    let ITCode_source = subPath.sourceCode;
                                    let ITCode_target = subPath.targetCode;
                                    if (isPerson_source != 1 && isPerson_source != '1') {
                                        newAllCodesOne.add(`${ITCode_source}`);
                                    }
                                    if (isPerson_target != 1 && isPerson_target != '1') {
                                        newAllCodesOne.add(`${ITCode_target}`);
                                    }
                                }
                            }
                        }
                        //处理直接被投资关系路径
                        else if (index == 1) {
                            let flag = isContinuousPath(from, to, pathArrayOne.path);
                            if (flag == true) {
                                promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                for (let subPath of pathArrayOne.path) {
                                    let isPerson_source = subPath.sourceIsPerson; 7
                                    let isPerson_target = subPath.targetIsPerson;
                                    let ITCode_source = subPath.sourceCode;
                                    let ITCode_target = subPath.targetCode;
                                    if (isPerson_source != 1 && isPerson_source != '1') {
                                        newAllCodesOne.add(`${ITCode_source}`);
                                    }
                                    if (isPerson_target != 1 && isPerson_target != '1') {
                                        newAllCodesOne.add(`${ITCode_target}`);
                                    }
                                }
                            }
                        }
                        //处理最短路径等其他关系路径
                        else {
                            promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                            for (let subPath of pathArrayOne.path) {
                                let isPerson_source = subPath.sourceIsPerson;
                                let isPerson_target = subPath.targetIsPerson;
                                let ITCode_source = subPath.sourceCode;
                                let ITCode_target = subPath.targetCode;
                                if (isPerson_source != 1 && isPerson_source != '1') {
                                    newAllCodesOne.add(`${ITCode_source}`);
                                }
                                if (isPerson_target != 1 && isPerson_target != '1') {
                                    newAllCodesOne.add(`${ITCode_target}`);
                                }
                            }
                        }
                    }
                    //from/to中存在自然人的情况, 不做处理
                    else if (from.toString().length != 10 || to.toString().length != 10) {
                        promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                        for (let subPath of pathArrayOne.path) {
                            let isPerson_source = subPath.sourceIsPerson;
                            let isPerson_target = subPath.targetIsPerson;
                            let ITCode_source = subPath.sourceCode;
                            let ITCode_target = subPath.targetCode;
                            if (isPerson_source != 1 && isPerson_source != '1') {
                                newAllCodesOne.add(`${ITCode_source}`);
                            }
                            if (isPerson_target != 1 && isPerson_target != '1') {
                                newAllCodesOne.add(`${ITCode_target}`);
                            }
                        }
                    }
                }
            }

            //处理担保关系--pathArrayTwo
            if (pathArrayTwo.hasOwnProperty('path') && pathArrayTwo.path.length > 0) {
                tempResultTwo.push(pathArrayTwo);
            }
        }

        uniqueCodesOne = Array.from(newAllCodesOne);

        //根据投资关系pathArrayOne中涉及到的机构才展示担保关系pathArrayTwo
        // for (let subResult of tempResultTwo) {
        //     let flag = true;
        //     for (let subPathDetail of subResult.path) {
        //         let sourceCode_two = subPathDetail.sourceCode;
        //         let targetCode_two = subPathDetail.targetCode;
        //         if (newAllCodesOne.has(`${sourceCode_two}`) == false || newAllCodesOne.has(`${targetCode_two}`) == false) {
        //             console.log('该担保关系的path不符合要求');
        //             flag = false;
        //             break;
        //         }
        //         allCodesTwo.add(`${sourceCode_two}`);
        //         allCodesTwo.add(`${targetCode_two}`);
        //     }
        //     if (flag == true) {
        //         //过滤重复的path
        //         let pathGroup = promiseResultTwo.dataDetail.data.pathDetail;
        //         let isDupFlag = isDuplicatedPath(pathGroup, subResult);
        //         if (isDupFlag == false) {
        //             promiseResultTwo.dataDetail.data.pathDetail.push(subResult);
        //         }
        //     }
        // }
        
        //根据投资关系pathArrayOne中涉及到的机构才展示担保关系pathArrayTwo
        for (let subResult of tempResultTwo) {
            let newSubResult = { path: [] };
            for (let subPathDetail of subResult.path) {
                let sourceCode_two = subPathDetail.sourceCode;
                let targetCode_two = subPathDetail.targetCode;
                if (newAllCodesOne.has(`${sourceCode_two}`) == true && newAllCodesOne.has(`${targetCode_two}`) == true) {
                    allCodesTwo.add(`${sourceCode_two}`);
                    allCodesTwo.add(`${targetCode_two}`);
                    newSubResult.path.push(subPathDetail);
                }
            }
            if (newSubResult.path.length > 0) {
                newTempResultTwo.push(newSubResult);
                //过滤重复的path
                let pathGroup = promiseResultTwo.dataDetail.data.pathDetail;
                let isDupFlag = isDuplicatedPath(pathGroup, newSubResult);
                if (isDupFlag == false) {
                    promiseResultTwo.dataDetail.data.pathDetail.push(newSubResult);
                }
            }
        }

        uniqueCodesTwo = Array.from(allCodesTwo);

        let retryCount = 0;
        do {
            try {
                if (uniqueCodesOne.length > 0) {
                    allNamesOne = await cacheHandlers.getAllNames(uniqueCodesOne);             //ITCode2->ITName
                }
                if (uniqueCodesTwo.length > 0) {
                    allNamesTwo = await cacheHandlers.getAllNames(uniqueCodesTwo);             //ITCode2->ITName
                }
                break;
            } catch (err) {
                retryCount++;
            }
        } while (retryCount < 3)
        if (retryCount == 3) {
            console.error('retryCount: 3, 批量查询机构名称失败');
            logger.error('retryCount: 3, 批量查询机构名称失败');
        }
        let newAllNamesOne = handlerAllNames(allNamesOne);                         // 处理ITName为空的情况
        let codeNameMapResOne = getCodeNameMapping(uniqueCodesOne, newAllNamesOne);   //获取ITCode2->ITName的Map
        promiseResultOne.mapRes = codeNameMapResOne;
        promiseResultOne.uniqueCodes = uniqueCodesOne;
        promiseResultOne.dataDetail.names = newAllNamesOne;
        promiseResultOne.dataDetail.codes = uniqueCodesOne;
        promiseResultOne.dataDetail.data.pathNum = promiseResultOne.dataDetail.data.pathDetail.length;

        let newAllNamesTwo = handlerAllNames(allNamesTwo);                         // 处理ITName为空的情况
        let codeNameMapResTwo = getCodeNameMapping(uniqueCodesTwo, newAllNamesTwo);   //获取ITCode2->ITName的Map
        promiseResultTwo.mapRes = codeNameMapResTwo;
        promiseResultTwo.uniqueCodes = uniqueCodesTwo;
        promiseResultTwo.dataDetail.names = newAllNamesTwo;
        promiseResultTwo.dataDetail.codes = uniqueCodesTwo;
        promiseResultTwo.dataDetail.data.pathNum = promiseResultTwo.dataDetail.data.pathDetail.length;

        promiseResult.pathTypeOne = promiseResultOne;
        promiseResult.pathTypeTwo = promiseResultTwo;

        return promiseResult;
    }
}

//从FullPathQuery返回的结果中筛选comInvestPath和comInvestedByPath
async function FilterComInvestRelationPath(subPath) {
    let comInvestedByPath = [];
    let comInvestPath = [];
    let commonRelationPath = {};
    let pathNum = subPath.data.pathNum;
    let subFullPath = subPath.data.pathDetail;
    let index = null;
    let isSourceRep = true;
    let isTargetRep = true;
    let comRelPathNum = 0;
    // let codes = new Set();
    for (let eachPath of subFullPath) {
        let sourceArray = [];
        let targetArray = [];
        for (let subEachPath of eachPath.path) {
            sourceArray.push(subEachPath.source);
            targetArray.push(subEachPath.target);
            // codes.add((subEachPath.sourceCode).toString(), (subEachPath.targetCode).toString());
        }
        isSourceRep = await isRepatition(sourceArray);
        isTargetRep = await isRepatition(targetArray);
        if (isSourceRep && isTargetRep) {
            continue;
        }
        else if (isSourceRep && !isTargetRep) {
            comInvestedByPath.push(eachPath);
            comRelPathNum++;
        }
        else if (isTargetRep && !isSourceRep) {
            comInvestPath.push(eachPath);
            comRelPathNum++;
        }

    }

    commonRelationPath.commonInvestPath = comInvestPath;
    commonRelationPath.commonInvestedByPath = comInvestedByPath;
    commonRelationPath.comRelationPathNum = comRelPathNum;
    // commonRelationPath.codes = Array.from(codes);
    return commonRelationPath;

}

//判断数组元素是否有重复元素
function isRepatition(arr) {
    let Arr = [];
    let repArr = [];
    for (let i = 0; i < arr.length; i++) {
        if (Arr.indexOf(arr[i]) == -1) {
            Arr.push(arr[i])
        } else {
            if (repArr.indexOf(arr[i]) == -1) {
                repArr.push(arr[i])
            }
        }
    }
    if (repArr.length >= 1)
        return true;
    else
        return false;
}

//为每个path追加source/target节点，即增加ITName属性
function setSourceTarget(map, pathDetail) {
    // let newPathDetail = [];
    try {
        for (let subPathDetail of pathDetail) {
            for (let subPath of subPathDetail.path) {
                let sourceName = null;
                if (subPath.sourceIsExtra == '1' || subPath.sourceIsExtra == 1 || subPath.sourceIsExtra == 'null') {
                    sourceName = subPath.source;
                } else if (subPath.sourceIsExtra == '0' || subPath.sourceIsExtra == 0) {
                    sourceName = map.get(`${subPath.sourceCode}`);
                }
                let targetName = null;
                if (subPath.targetIsExtra == '1' || subPath.targetIsExtra == 1 || subPath.targetIsExtra == 'null') {
                    // targetName = subPath.targetIsExtra;
                    targetName = subPath.target;
                } else if (subPath.targetIsExtra == '0' || subPath.targetIsExtra == 0) {
                    targetName = map.get(`${subPath.targetCode}`);
                }
                subPath.source = sourceName;
                subPath.target = targetName;
            }
        }
        return pathDetail;
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }

}

//通过注册资本区间值筛选路径
function filterPathAccRegFund(paths, lowFund, highFund) {
    let newPaths = [];
    try {
        if (lowFund && highFund) {                                                     //lowFund和highFund存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                           //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(sourceRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund < lowFund || subRegFund > highFund) {            //如果subRegFund不在low和hign范围内，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund >= lowFund && subRegFund <= highFund) {    //如果subRegFund在low和hign范围内，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
        else if (lowFund && !highFund) {                                               //lowFund存在highFund不存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                            //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(sourceRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund < lowFund) {            //如果subRegFund>low，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund >= lowFund) {    //如果subRegFund<=low，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
        else if (!lowFund && highFund) {                                               //lowFund不存在highFund存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                            //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(sourceRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund > highFund) {                                    //如果subRegFund>hign，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund <= highFund) {                           //如果subRegFund<=hign，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
    return newPaths;
}

//处理neo4j server 返回的结果
async function handlerNeo4jResult(from, to, resultPromise, j) {
    let queryNodeResult = { nodeResultOne: { pathDetail: {} }, nodeResultTwo: { pathDetail: {} } };
    let handlerPromiseStart = Date.now();
    let promiseResult = await handlerPromise(from, to, resultPromise, j);
    let handlerPromiseCost = Date.now() - handlerPromiseStart;
    console.log('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    logger.info('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    let beforePathDetailOne = promiseResult.pathTypeOne.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    let beforePathDetailTwo = promiseResult.pathTypeTwo.dataDetail.data.pathDetail;
    let setSourceTargetStart = Date.now();
    let afterPathDetailOne = setSourceTarget(promiseResult.pathTypeOne.mapRes, beforePathDetailOne);
    let afterPathDetailTwo = setSourceTarget(promiseResult.pathTypeTwo.mapRes, beforePathDetailTwo);
    let setSourceTargetCost = Date.now() - setSourceTargetStart;
    console.log('setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    logger.info('setSourceTargetCost: ' + setSourceTargetCost + 'ms');

    promiseResult.pathTypeOne.dataDetail.data.pathDetail = afterPathDetailOne;                       //用带ITName的pathDetail替换原来的    
    queryNodeResult.nodeResultOne.pathDetail = promiseResult.pathTypeOne.dataDetail;

    promiseResult.pathTypeTwo.dataDetail.data.pathDetail = afterPathDetailTwo;                       //用带ITName的pathDetail替换原来的    
    queryNodeResult.nodeResultTwo.pathDetail = promiseResult.pathTypeTwo.dataDetail;
    // if (j != 4 && j != 5) {
    //     queryNodeResult.pathDetail = promiseResult.dataDetail;
    // }
    // else if (j == 4) {
    //     let now = Date.now();
    //     let resultIndex = `result_${j}`;
    //     let pathTypeName = "CommonInvestPath";
    //     let filterResult = await FilterComInvestRelationPath(promiseResult.dataDetail);
    //     let commonInvestPathQueryCost = Date.now() - now;
    //     logger.info("CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
    //     console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
    //     let comInvestPath = filterResult.commonInvestPath;
    //     let dataResult = { dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] } };
    //     // let dataResults = [];
    //     if (comInvestPath.length == 0) {
    //         // dataResults.push({ data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] });               
    //     } else if (comInvestPath.length > 0) {
    //         // let allNames = [];
    //         let allNames = new Set();
    //         let distinctPathNames = [];
    //         let detail = [];
    //         let allCodes = new Set();
    //         dataResult.dataDetail.data.pathNum = comInvestPath.length;
    //         for (let subComInvestPath of comInvestPath) {
    //             for (let eachComPath of subComInvestPath.path) {
    //                 let sourceName = eachComPath.source;
    //                 let targetName = eachComPath.target;
    //                 // allNames.push(sourceName, targetName);
    //                 allNames.add(sourceName);
    //                 allNames.add(targetName);
    //                 allCodes.add(`${eachComPath.sourceCode}`);
    //                 allCodes.add(`${eachComPath.targetCode}`);
    //             }
    //             detail.push(subComInvestPath);
    //         }
    //         // distinctPathNames = unique(allNames);
    //         distinctPathNames = Array.from(allNames);
    //         dataResult.dataDetail.names = distinctPathNames;
    //         dataResult.dataDetail.codes = Array.from(allCodes);
    //         dataResult.dataDetail.data.pathDetail = detail;
    //         // dataResults.push(dataResult.dataDetail);   
    //     }
    //     queryNodeResult.pathDetail = dataResult;
    // }
    // else if (j == 5) {
    //     let now = Date.now();
    //     let resultIndex = `result_${j}`;
    //     let pathTypeName = "CommonInvestedByPath";
    //     let filterResult = await FilterComInvestRelationPath(promiseResult.dataDetail);
    //     let commonInvestedByPathQueryCost = Date.now() - now;
    //     logger.info("CommonInvestedByPathQueryCost: " + commonInvestedByPathQueryCost + 'ms');
    //     console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", CommonInvestedByPathQueryCost: " + commonInvestedByPathQueryCost + 'ms');
    //     let comInvestedByPath = filterResult.commonInvestedByPath;
    //     let dataResult = { dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] } };
    //     // let dataResults = [];
    //     if (comInvestedByPath.length == 0) {
    //         // dataResults.push({ data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] });                        
    //     }
    //     else if (comInvestedByPath.length > 0) {
    //         // let allNames = [];
    //         let allNames = new Set();
    //         let distinctPathNames = [];
    //         let detail = [];
    //         let allCodes = new Set();
    //         dataResult.dataDetail.data.pathNum = comInvestedByPath.length;
    //         for (let subComInvestedByPath of comInvestedByPath) {
    //             for (let eachComPath of subComInvestedByPath.path) {
    //                 let sourceName = eachComPath.source;
    //                 let targetName = eachComPath.target;
    //                 // allNames.push(sourceName, targetName);
    //                 allNames.add(sourceName);
    //                 allNames.add(targetName);
    //                 allCodes.add(`${eachComPath.sourceCode}`);
    //                 allCodes.add(`${eachComPath.targetCode}`);
    //             }
    //             detail.push(subComInvestedByPath);
    //         }
    //         // distinctPathNames = unique(allNames);
    //         distinctPathNames = Array.from(allNames);
    //         dataResult.dataDetail.names = distinctPathNames;
    //         dataResult.dataDetail.codes = Array.from(allCodes);
    //         dataResult.dataDetail.data.pathDetail = detail;
    //         // dataResults.push(dataResult.dataDetail);
    //     }
    //     queryNodeResult.pathDetail = dataResult;
    // }
    return queryNodeResult;
}

//处理neo4j session1
function sessionRun1(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun1 session close!');
        return result;
    });
}

//处理neo4j session2
function sessionRun2(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun2 session close!');
        return result;
    });
}

//处理neo4j session3
function sessionRun3(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun3 session close!');
        return result;
    });
}

//处理neo4j session4
function sessionRun4(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun4 session close!');
        return result;
    });
}

//处理neo4j session5
function sessionRun5(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun5 session close!');
        return result;
    });
}

//处理neo4j session6
function sessionRun6(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun6 session close!');
        return result;
    });
}

//处理neo4j session7
function sessionRun7(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun7 session close!');
        return result;
    });
}

//处理neo4j session8
function sessionRun8(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun8 session close!');
        return result;
    });
}

let searchGraph = {

    //6种方式的路径同时查询
    // queryNodePath: async function (from, to, IVDepth, IVBDepth, FUDepth, lowWeight, highWeight) {
    //     return new Promise(async function (resolve, reject) {

    //         try {
    //             let allPathNum = 0;
    //             let nodeIdOne = await findNodeId(from);
    //             let nodeIdTwo = await findNodeId(to);
    //             //分别定义4种查询方式的请求体
    //             let investPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)-[r:invests*..${IVDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             let investedByPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)<-[r:invests*..${IVBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             let shortestPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= allShortestPaths((from)-[r:invests*]-(to)) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             let fullPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)-[r:invests*..${FUDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             let resultPromise = [];
    //             if (nodeIdOne == null || nodeIdTwo == null) {
    //                 console.error(`${from} and ${to} is not in the neo4j database at all !`);
    //                 logger.error(`${from} and ${to} is not in the neo4j database at all !`);
    //                 return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
    //             }
    //             else {
    //                 //将每种方式查询出来的路径放入一个数组中
    //                 let now = 0;
    //                 now = Date.now();
    //                 resultPromise.push(await session.run(investPathQuery));
    //                 let investPathQueryCost = Date.now() - now;
    //                 logger.info("InvestPathQueryCost: " + investPathQueryCost + 'ms');
    //                 console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", InvestPathQueryCost: " + investPathQueryCost + 'ms');

    //                 now = Date.now();
    //                 resultPromise.push(await session.run(investedByPathQuery));
    //                 let investedByPathQueryCost = Date.now() - now;
    //                 logger.info("InvestedByPathQueryCost: " + investedByPathQueryCost + 'ms');
    //                 console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", InvestedByPathQueryCost: " + investedByPathQueryCost + 'ms');
    //                 now = Date.now();
    //                 resultPromise.push(await session.run(shortestPathQuery));
    //                 let shortestPathQueryCost = Date.now() - now;
    //                 logger.info("ShortestPathQueryCost: " + shortestPathQueryCost + 'ms');
    //                 console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", ShortestPathQueryCost: " + shortestPathQueryCost + 'ms');

    //             }

    //             //先处理investPath/investedByPath/shortestPath的数据格式
    //             let dataResults = [];
    //             let pathTypeName = "";
    //             let queryNodeResult = { dataDetail: dataResults, totalPathNum: allPathNum };
    //             let j = 0;                  //记录每种路径查询方式索引号
    //             let resultIndex = null;

    //             for (let subResultPromise of resultPromise) {
    //                 let res = await handlerPromise(subResultPromise, j);
    //                 let beforePathDetail = res.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    //                 let afterPathDetail = setSourceTarget(res.mapRes, beforePathDetail);
    //                 res.dataDetail.data.pathDetail = afterPathDetail;                       //用带ITName的pathDetail替换原来的
    //                 j++;
    //                 dataResults.push(res.dataDetail);
    //                 allPathNum += res.toPaNum;
    //             }

    //             now = Date.now();
    //             // let fullPathPromise = session.run(fullPathQuery); 
    //             // let fullPathRes = fullPathSessionRun(fullPathQuery, session); 
    //             let fullPathRes = await session.run(fullPathQuery);
    //             session.close();
    //             //当fullPathQuery等待时间较长时，认为找不到结果
    //             if (fullPathRes) {
    //                 let pathTypeName = "";
    //                 let fullPathQueryCost = Date.now() - now;
    //                 logger.info("FullPathQueryCost: " + fullPathQueryCost + 'ms');
    //                 console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", FullPathQueryCost: " + fullPathQueryCost + 'ms');

    //                 let fullPath = await handlerPromise(fullPathRes, j);
    //                 let beforePathDetail = fullPath.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    //                 let afterPathDetail = setSourceTarget(fullPath.mapRes, beforePathDetail);
    //                 fullPath.dataDetail.data.pathDetail = afterPathDetail;                       //用带ITName的pathDetail替换原来的
    //                 dataResults.push(fullPath.dataDetail);
    //                 allPathNum += fullPath.toPaNum;
    //                 j++;
    //                 //处理comInvestPath和comInvestedByPath
    //                 let res = await FilterComInvestRelationPath(fullPath.dataDetail);
    //                 let comInvestPath = res.commonInvestPath;
    //                 let comInvestedByPath = res.commonInvestedByPath;
    //                 allPathNum += res.comRelationPathNum;

    //                 resultIndex = `result_${j}`;
    //                 if (j == 4)
    //                     pathTypeName = "CommonInvestPath";
    //                 let dataResult = { toPaNum: allPathNum, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] } };
    //                 if (comInvestPath.length == 0) {
    //                     // dataResults.push({data: {pathDetail: [{path: [], name: " "}], pathNum: 0}, type: resultIndex, typeName: pathTypeName, names: []});
    //                     dataResults.push({ data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [] });
    //                 } else {

    //                     let allNames = [];
    //                     let distinctPathNames = null;
    //                     // let dataResult = {toPaNum: allPathNum, dataDetail: {data: {pathDetail: [], pathNum: 0}, type: resultIndex, names: []}};
    //                     let detail = [];
    //                     dataResult.dataDetail.data.pathNum = comInvestPath.length;
    //                     for (let subComInvestPath of comInvestPath) {
    //                         for (let eachComPath of subComInvestPath.path) {
    //                             let sourceName = eachComPath.source;
    //                             let targetName = eachComPath.target;
    //                             allNames.push(sourceName, targetName);
    //                         }
    //                         detail.push(subComInvestPath);
    //                     }
    //                     distinctPathNames = unique(allNames);
    //                     dataResult.dataDetail.names = distinctPathNames;
    //                     dataResult.dataDetail.data.pathDetail = detail;
    //                     // dataResult.data.pathDetail.name = " "; 
    //                     dataResults.push(dataResult.dataDetail);
    //                 }
    //                 j++;

    //                 resultIndex = `result_${j}`;
    //                 if (j == 5)
    //                     pathTypeName = "CommonInvestedByPath";
    //                 if (comInvestedByPath.length == 0) {
    //                     // dataResults.push({data: {pathDetail: [{path: [], name: " "}], pathNum: 0}, type: resultIndex, typeName: pathTypeName, names: []});
    //                     dataResults.push({ data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] });

    //                 } else {
    //                     let allNames = [];
    //                     let distinctPathNames = null;
    //                     let dataResult = { toPaNum: allPathNum, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: resultIndex, typeName: pathTypeName, names: [], codes: [] } };
    //                     let detail = [];
    //                     dataResult.dataDetail.data.pathNum = comInvestedByPath.length;
    //                     for (let subComInvestedByPath of comInvestedByPath) {
    //                         for (let eachComPath of subComInvestedByPath.path) {
    //                             let sourceName = eachComPath.source;
    //                             let targetName = eachComPath.target;
    //                             allNames.push(sourceName, targetName);
    //                         }
    //                         detail.push(subComInvestedByPath);
    //                     }
    //                     distinctPathNames = unique(allNames);
    //                     dataResult.dataDetail.names = distinctPathNames;
    //                     dataResult.dataDetail.data.pathDetail = detail;
    //                     // dataResult.data.pathDetail.name = " ";  
    //                     dataResults.push(dataResult.dataDetail);
    //                 }
    //                 queryNodeResult.dataDetail = dataResults;
    //                 queryNodeResult.totalPathNum = dataResult.toPaNum;
    //                 // driver.close();
    //                 // session.close();
    //                 return resolve(queryNodeResult);

    //             }
    //             else {
    //                 // return reply.response(errorResp(errorCode.INTERNALERROR, `Could not return full path's results !`));
    //                 return resolve({});
    //             }

    //         } catch (err) {
    //             console.error(err);
    //             logger.error(err);
    //             // session.close();
    //             // driver.close();
    //             return reject(err);

    //         }
    //     }
    //     )
    //     // .timeout(lookupTimeout).catch( err => {
    //     //     console.log(err);
    //     //     logger.info(err);
    //     //     // return reject(err);
    //     // });  
    // },

    //直接投资关系的路径查询
    queryInvestPath: async function (from, to, IVDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 0;                                                                                   //记录每种路径查询方式索引号,investPathQuery索引为0
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, IVDepth, j, pathType].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, IVDepth, lowWeight, highWeight, pathType].join('-');
                if (!warmUpValue) {
                    // let nodeIdOne = await findNodeId(from);
                    // let nodeIdTwo = await findNodeId(to);
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let fromOutBoundNum = await cacheHandlers.getCache(`${from}-OutBound`);
                        if (!fromOutBoundNum) {
                            let fromOutBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    fromOutBoundNum = await getOutBoundNodeNum(from, IVDepth);                                   //获取from的outbound节点数
                                     break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getOutBoundNodeNum execute fail after trying 3 times: ' +from);
                                logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' +from);
                            }
                            let fromOutBoundNumCost = Date.now() - fromOutBoundNumStart;
                            console.log(`${from} fromOutBoundNumCost: ` + fromOutBoundNumCost + 'ms' + ', fromOutBoundNum: ' + fromOutBoundNum);
                            cacheHandlers.setCache(`${from}-OutBound`, fromOutBoundNum);
                        }
                        let toInBoundNum = await cacheHandlers.getCache(`${to}-InBound`);
                        if (!toInBoundNum) {
                            let toInBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    toInBoundNum = await getInBoundNodeNum(to, IVDepth);                                    //获取to的inbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getInBoundNodeNum execute fail after trying 3 times: ' +to);
                                logger.error('getInBoundNodeNum execute fail after trying 3 times: ' +to);
                            }
                            let toInBoundNumCost = Date.now() - toInBoundNumStart;
                            console.log(`${to} toInBoundNumCost: ` + toInBoundNumCost + 'ms' + ', toInBoundNum: ' + toInBoundNum);
                            cacheHandlers.setCache(`${to}-InBound`, toInBoundNum);
                        }
                        let investPathQuery = null;
                        let investPathQuery_outBound = null;
                        let investPathQuery_inBound = null;
                        // let investPathQuery_outBound = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo}) match p= (from)-[r:invests*..${IVDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // let investPathQuery_inBound = `start to=node(${nodeIdOne}), from=node(${nodeIdTwo}) match p= (from)<-[r:invests*..${IVDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // let investPathQuery_outBound = `match p= (from:company{ITCode2: ${from}})-[r:invests*..${IVDepth}]->(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        // let investPathQuery_inBound = `match p= (from:company{ITCode2: ${to}})<-[r:invests*..${IVDepth}]-(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        if (pathType == 'invests' || !pathType) {
                            investPathQuery_outBound = `match p= (from:company{ITCode2: ${from}})-[r:invests*..${IVDepth}]->(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                            investPathQuery_inBound = `match p= (from:company{ITCode2: ${to}})<-[r:invests*..${IVDepth}]-(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        }
                        else if (pathType == 'all') {
                            investPathQuery_outBound = `match p= (from:company{ITCode2: ${from}})-[r:invests|guarantees*..${IVDepth}]->(to:company{ITCode2: ${to}}) return p`;
                            investPathQuery_inBound = `match p= (from:company{ITCode2: ${to}})<-[r:invests|guarantees*..${IVDepth}]-(to:company{ITCode2: ${from}}) return p`;
                        }
                        if (fromOutBoundNum > toInBoundNum) {
                            investPathQuery = investPathQuery_outBound;
                        } else if (fromOutBoundNum <= toInBoundNum) {
                            investPathQuery = investPathQuery_inBound;
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            now = Date.now();
                            // resultPromise = await session.run(investPathQuery);

                            let retryCount = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun1(investPathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun1 execute fail after trying 3 times: ' +investPathQuery);
                                logger.error('sessionRun1 execute fail after trying 3 times: ' +investPathQuery);
                            }
                            console.log('query neo4j server: ' +investPathQuery);
                            logger.info('query neo4j server: ' +investPathQuery);
                            let investPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " InvestPathQueryCost: " + investPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", InvestPathQueryCost: " + investPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(from, to, resultPromise, j);
                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (investPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [IVDepth], relations: [j], cost: investPathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    //直接被投资关系的路径查询
    queryInvestedByPath: async function (from, to, IVBDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 1;                                                            //记录每种路径查询方式索引号,investByPathQuery索引为1
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, IVBDepth, j, pathType].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, IVBDepth, lowWeight, highWeight, pathType].join('-');
                if (!warmUpValue) {
                    // let nodeIdOne = await findNodeId(from);
                    // let nodeIdTwo = await findNodeId(to);
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestedByPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let fromInBoundNum = await cacheHandlers.getCache(`${from}-InBound`);
                        if (!fromInBoundNum) {
                            let fromInBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    fromInBoundNum = await getInBoundNodeNum(from, IVBDepth);                                     //获取from的inbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getInBoundNodeNum execute fail after trying 3 times: ' +from);
                                logger.error('getInBoundNodeNum execute fail after trying 3 times: ' +from);
                            }
                            let fromInBoundNumCost = Date.now() - fromInBoundNumStart;
                            console.log(`${from} fromInBoundNumCost: ` + fromInBoundNumCost + 'ms' + ', fromInBoundNum: ' + fromInBoundNum);
                            cacheHandlers.setCache(`${from}-InBound`, fromInBoundNum);
                        }
                        let toOutBoundNum = await cacheHandlers.getCache(`${to}-OutBound`);
                        if (!toOutBoundNum) {
                            let toOutBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    toOutBoundNum = await getOutBoundNodeNum(to, IVBDepth);                                     //获取to的outbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getOutBoundNodeNum execute fail after trying 3 times: ' +to);
                                logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' +to);
                            }
                            let toOutBoundNumCost = Date.now() - toOutBoundNumStart;
                            console.log(`${to} toOutBoundNumCost: ` + toOutBoundNumCost + 'ms' + ', toOutBoundNum: ' + toOutBoundNum);
                            cacheHandlers.setCache(`${to}-OutBound`, toOutBoundNum);
                        }
                        let investedByPathQuery = null;
                        let investedByPathQuery_inBound = null;
                        let investedByPathQuery_outBound = null;
                        // let investedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        // let investedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;    
                        if (pathType == 'invests' || !pathType) {
                            investedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                            investedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        }
                        else if (pathType == 'all') {
                            investedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:invests|guarantees*..${IVBDepth}]-(to:company{ITCode2: ${to}}) return p`;
                            investedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:invests|guarantees*..${IVBDepth}]->(to:company{ITCode2: ${from}}) return p`;
                        }

                        // let investedByPathQuery_inBound = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo}) match p= (from)<-[r:invests*..${IVBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // let investedByPathQuery_outBound = `start to=node(${nodeIdOne}), from=node(${nodeIdTwo}) match p= (from)-[r:invests*..${IVBDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // if (isExtra == 0 || isExtra == '0') {
                        //     investedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        //     investedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;                            
                        // } else {
                        //     investedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //     investedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // }
                        if (toOutBoundNum < fromInBoundNum) {
                            investedByPathQuery = investedByPathQuery_inBound;
                        } else if (toOutBoundNum >= fromInBoundNum) {
                            investedByPathQuery = investedByPathQuery_outBound;
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            now = Date.now();
                            // resultPromise = await session.run(investedByPathQuery);

                            let retryCount = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun2(investedByPathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun2 execute fail after trying 3 times: ' +investedByPathQuery);
                                logger.error('sessionRun2 execute fail after trying 3 times: ' +investedByPathQuery);
                            }                           
                            console.log('query neo4j server: ' +investedByPathQuery);
                            logger.info('query neo4j server: ' +investedByPathQuery);
                            let investedByPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " InvestedByPathQueryCost: " + investedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", InvestedByPathQueryCost: " + investedByPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(from, to, resultPromise, j);
                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (investedByPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [IVBDepth], relations: [j], cost: investedByPathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestedByPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    //最短路径
    queryShortestPath: async function (from, to, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 2;                                                             //记录每种路径查询方式索引号,ShortestPathQuery索引为2
                let cacheKey = [j, from, to, lowWeight, highWeight, pathType].join('-');
                // let nodeIdOne = await findNodeId(from);
                // let nodeIdTwo = await findNodeId(to);
                // let shortestPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= allShortestPaths((from)-[r:invests*]-(to)) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                // let shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: ${from}})-[r:invests*]-(to:company{ITCode2: ${to}})) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                let shortestPathQuery = null;
                if (pathType == 'invests' || !pathType) {
                    shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: ${from}})-[r:invests*]-(to:company{ITCode2: ${to}})) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p limit 10`;
                }
                else if (pathType == 'all') {
                    shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: ${from}})-[r:invests|guarantees*]-(to:company{ITCode2: ${to}})) return p limit 10`;
                }
                if (from == null || to == null) {
                    console.error(`${from} or ${to} is not in the neo4j database at all !`);
                    logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                    // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                    let nodeResults = {
                        nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'ShortestPath', names: [], codes: [] } },
                        nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                    };
                    return resolve(nodeResults);
                }
                else if (from != null && to != null) {
                    let now = 0;
                    //缓存
                    let previousValue = await cacheHandlers.getCache(cacheKey);
                    if (!previousValue) {
                        now = Date.now();
                        // resultPromise = await session.run(shortestPathQuery);

                        let retryCount = 0;
                        let resultPromise = null;
                        do {
                            try {
                                resultPromise = await sessionRun3(shortestPathQuery);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('sessionRun3 execute fail after trying 3 times: ' +shortestPathQuery);
                            logger.error('sessionRun3 execute fail after trying 3 times: ' +shortestPathQuery);
                        } 
                        console.log('query neo4j server: ' +shortestPathQuery);
                        logger.info('query neo4j server: ' +shortestPathQuery);
                        let shortestPathQueryCost = Date.now() - now;
                        logger.info(`from: ${from} to: ${to}` + " ShortestPathQueryCost: " + shortestPathQueryCost + 'ms');
                        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", ShortestPathQueryCost: " + shortestPathQueryCost + 'ms');
                        if (resultPromise.records.length > 0) {
                            let result = await handlerNeo4jResult(from, to, resultPromise, j);
                            cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                            return resolve(result);
                        } else if (resultPromise.records.length == 0) {
                            let nodeResults = {
                                nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'ShortestPath', names: [], codes: [] } },
                                nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                            };
                            cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                            return resolve(nodeResults);
                        }
                    }
                    else if (previousValue) {
                        return resolve(JSON.parse(previousValue));
                    }
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    //全部路径
    queryfullPath: async function (from, to, FUDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 3;                                                             //记录每种路径查询方式索引号,fullPathQuery索引为3
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, FUDepth, j, pathType].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, FUDepth, lowWeight, highWeight, pathType].join('-');
                if (!warmUpValue) {
                    // let nodeIdOne = await findNodeId(from);
                    // let nodeIdTwo = await findNodeId(to);
                    // let fullPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)-[r:invests*..${FUDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                    // let fullPathQuery = `match p= (from:company{ITCode2: ${from}})-[r:invests*..${FUDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;  
                    let fullPathQuery = null;
                    if (pathType == 'invests' || !pathType) {
                        fullPathQuery = `match p= (from:company{ITCode2: ${from}})-[r:invests*..${FUDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                    }
                    else if (pathType == 'all') {
                        fullPathQuery = `match p= (from:company{ITCode2: ${from}})-[r:invests|guarantees*..${FUDepth}]-(to:company{ITCode2: ${to}}) return p`;
                    }

                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'FullPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            now = Date.now();
                            // resultPromise = await session.run(fullPathQuery);

                            let retryCount = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun4(fullPathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun4 execute fail after trying 3 times: ' +fullPathQuery);
                                logger.error('sessionRun4 execute fail after trying 3 times: ' +fullPathQuery);
                            } 
                            console.log('query neo4j server: ' +fullPathQuery);
                            logger.info('query neo4j server: ' +fullPathQuery);
                            let fullPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " FullPathQueryCost: " + fullPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", FullPathQueryCost: " + fullPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(from, to, resultPromise, j);

                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (fullPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [FUDepth], relations: [j], cost: fullPathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            } else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'FullPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    //共同投资关系路径查询
    queryCommonInvestPath: async function (from, to, CIVDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 4;
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, CIVDepth, j, pathType].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, CIVDepth, lowWeight, highWeight, pathType].join('-');
                if (!warmUpValue) {
                    // let nodeIdOne = await findNodeId(from);
                    // let nodeIdTwo = await findNodeId(to);
                    // let fullPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)-[r:invests*..${FUDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                    // let commonInvestPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)-[r1:invests*..${CIVDepth/2}]->()<-[r2:invests*..${CIVDepth/2}]-(to) return p`;
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        // let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestPath', names: [], codes: [] } };
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let fromOutBoundNum = await cacheHandlers.getCache(`${from}-OutBound`);
                        if (!fromOutBoundNum) {
                            let fromOutBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    fromOutBoundNum = await getOutBoundNodeNum(from, CIVDepth);                                   //获取from的outbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getOutBoundNodeNum execute fail after trying 3 times: ' +from);
                                logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' +from);
                            }
                            let fromOutBoundNumCost = Date.now() - fromOutBoundNumStart;
                            console.log(`${from} fromOutBoundNumCost: ` + fromOutBoundNumCost + 'ms' + ', fromOutBoundNum: ' + fromOutBoundNum);
                            cacheHandlers.setCache(`${from}-OutBound`, fromOutBoundNum);
                        }
                        let toInBoundNum = await cacheHandlers.getCache(`${to}-InBound`);
                        if (!toInBoundNum) {
                            let toInBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    toInBoundNum = await getInBoundNodeNum(to, CIVDepth);                                    //获取to的inbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getInBoundNodeNum execute fail after trying 3 times: ' +to);
                                logger.error('getInBoundNodeNum execute fail after trying 3 times: ' +to);
                            }
                            let toInBoundNumCost = Date.now() - toInBoundNumStart;
                            console.log(`${to} toInBoundNumCost: ` + toInBoundNumCost + 'ms' + ', toInBoundNum: ' + toInBoundNum);
                            cacheHandlers.setCache(`${to}-InBound`, toInBoundNum);
                        }
                        let commonInvestPathQuery = null;
                        // let commonInvestPathQueryOne = `match p= (from:company{ITCode2: ${from}})-[r1:invests*..${CIVDepth/2}]->()<-[r2:invests*..${CIVDepth/2}]-(to:company{ITCode2: ${to}}) where from.isExtra = '0' and to.isExtra = '0' return p`;                    
                        // let commonInvestPathQueryTwo = `match p= (from:company{ITCode2: ${to}})-[r1:invests*..${CIVDepth/2}]->()<-[r2:invests*..${CIVDepth/2}]-(to:company{ITCode2: ${from}}) where from.isExtra = '0' and to.isExtra = '0' return p`;                    
                        let commonInvestPathQueryOne = null;
                        let commonInvestPathQueryTwo = null;
                        if (pathType == 'invests' || !pathType) {
                            commonInvestPathQueryOne = `match p= (from:company{ITCode2: ${from}})-[r1:invests*..${CIVDepth / 2}]->()<-[r2:invests*..${CIVDepth / 2}]-(to:company{ITCode2: ${to}}) return p`;
                            commonInvestPathQueryTwo = `match p= (from:company{ITCode2: ${to}})-[r1:invests*..${CIVDepth / 2}]->()<-[r2:invests*..${CIVDepth / 2}]-(to:company{ITCode2: ${from}}) return p`;
                        }
                        else if (pathType == 'all') {
                            commonInvestPathQueryOne = `match p= (from:company{ITCode2: ${from}})-[r1:invests|guarantees*..${CIVDepth / 2}]->()<-[r2:invests|guarantees*..${CIVDepth / 2}]-(to:company{ITCode2: ${to}}) return p`;
                            commonInvestPathQueryTwo = `match p= (from:company{ITCode2: ${to}})-[r1:invests|guarantees*..${CIVDepth / 2}]->()<-[r2:invests|guarantees*..${CIVDepth / 2}]-(to:company{ITCode2: ${from}}) return p`;
                        }
                        if (fromOutBoundNum > toInBoundNum) {
                            commonInvestPathQuery = commonInvestPathQueryOne;
                        } else if (fromOutBoundNum <= toInBoundNum) {
                            commonInvestPathQuery = commonInvestPathQueryTwo;
                        }
                        let now = 0;
                        //缓存
                        // let key = [j, commonInvestPathQuery].join('-');
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let commonInvestPathQueryCost = 0;
                            now = Date.now();
                            // resultPromise = await session.run(commonInvestPathQuery);

                            let retryCount = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun5(commonInvestPathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun5 execute fail after trying 3 times: ' +commonInvestPathQuery);
                                logger.error('sessionRun5 execute fail after trying 3 times: ' +commonInvestPathQuery);
                            } 
                            console.log('query neo4j server: ' +commonInvestPathQuery);
                            logger.info('query neo4j server: ' +commonInvestPathQuery);
                            commonInvestPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                logger.info(`from: ${from} to: ${to}` + " CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
                                let result = await handlerNeo4jResult(from, to, resultPromise, j);
                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (commonInvestPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [CIVDepth], relations: [j], cost: commonInvestPathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            } else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    //共同被投资关系路径查询
    queryCommonInvestedByPath: async function (from, to, CIVBDepth, lowWeight, highWeight, isExtra, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 5;
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, CIVBDepth, j, isExtra, pathType].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, CIVBDepth, lowWeight, highWeight, isExtra, pathType].join('-');
                if (!warmUpValue) {
                    // let nodeIdOne = await findNodeId(from);
                    // let nodeIdTwo = await findNodeId(to);
                    // let fullPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)-[r:invests*..${FUDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                    // let commonInvestedByPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= (from)<-[r1:invests*..${CIVBDepth/2}]-()-[r2:invests*..${CIVBDepth/2}]->(to) return p`;
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestedByPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let fromInBoundNum = await cacheHandlers.getCache(`${from}-InBound`);
                        if (!fromInBoundNum) {
                            let fromInBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    fromInBoundNum = await getInBoundNodeNum(from, CIVBDepth);                                     //获取from的inbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getInBoundNodeNum execute fail after trying 3 times: ' +from);
                                logger.error('getInBoundNodeNum execute fail after trying 3 times: ' +from);
                            }
                            let fromInBoundNumCost = Date.now() - fromInBoundNumStart;
                            console.log(`${from} fromInBoundNumCost: ` + fromInBoundNumCost + 'ms' + ', fromInBoundNum: ' + fromInBoundNum);
                            cacheHandlers.setCache(`${from}-InBound`, fromInBoundNum);
                        }
                        let toOutBoundNum = await cacheHandlers.getCache(`${to}-OutBound`);
                        if (!toOutBoundNum) {
                            let toOutBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    toOutBoundNum = await getOutBoundNodeNum(to, CIVBDepth);                                     //获取to的outbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getOutBoundNodeNum execute fail after trying 3 times: ' +to);
                                logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' +to);
                            }
                            let toOutBoundNumCost = Date.now() - toOutBoundNumStart;
                            console.log(`${to} toOutBoundNumCost: ` + toOutBoundNumCost + 'ms' + ', toOutBoundNum: ' + toOutBoundNum);
                            cacheHandlers.setCache(`${to}-OutBound`, toOutBoundNum);
                        }
                        let commonInvestedByPathQuery = null;
                        let commonInvestedByPathQueryOne = null;
                        let commonInvestedByPathQueryTwo = null;
                        if (pathType == 'invests' || !pathType) {
                            if (isExtra == 0 || isExtra == '0') {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: ${from}})<-[r1:invests*..${CIVBDepth / 2}]-(com)-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: ${to}}) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: ${to}})<-[r1:invests*..${CIVBDepth / 2}]-(com)-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: ${from}}) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                            }
                            else {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: ${from}})<-[r1:invests*..${CIVBDepth / 2}]-()-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: ${to}}) return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: ${to}})<-[r1:invests*..${CIVBDepth / 2}]-()-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: ${from}}) return p`;
                            }
                        }
                        else if (pathType == 'all') {
                            if (isExtra == 0 || isExtra == '0') {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: ${from}})<-[r1:invests|guarantees*..${CIVBDepth / 2}]-(com)-[r2:invests|guarantees*..${CIVBDepth / 2}]->(to:company{ITCode2: ${to}}) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: ${to}})<-[r1:invests|guarantees*..${CIVBDepth / 2}]-(com)-[r2:invests|guarantees*..${CIVBDepth / 2}]->(to:company{ITCode2: ${from}}) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                            }
                            else {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: ${from}})<-[r1:invests|guarantees*..${CIVBDepth / 2}]-()-[r2:invests|guarantees*..${CIVBDepth / 2}]->(to:company{ITCode2: ${to}}) return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: ${to}})<-[r1:invests|guarantees*..${CIVBDepth / 2}]-()-[r2:invests|guarantees*..${CIVBDepth / 2}]->(to:company{ITCode2: ${from}}) return p`;
                            }
                        }

                        if (toOutBoundNum < fromInBoundNum) {
                            commonInvestedByPathQuery = commonInvestedByPathQueryOne;
                        } else if (toOutBoundNum >= fromInBoundNum) {
                            commonInvestedByPathQuery = commonInvestedByPathQueryTwo;
                        }
                        let now = 0;
                        //缓存
                        // let key = [j, commonInvestedByPathQuery].join('-');
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let commonInvestedByPathQueryCost = 0;
                            now = Date.now();
                            // resultPromise = await session.run(commonInvestedByPathQuery);

                            let retryCount = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun6(commonInvestedByPathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun6 execute fail after trying 3 times: ' +commonInvestedByPathQuery);
                                logger.error('sessionRun6 execute fail after trying 3 times: ' +commonInvestedByPathQuery);
                            }    
                            console.log('query neo4j server: ' +commonInvestedByPathQuery);
                            logger.info('query neo4j server: ' +commonInvestedByPathQuery);
                            commonInvestedByPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " CommonInvestedByPathQueryCost: " + commonInvestedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", CommonInvestedByPathQueryCost: " + commonInvestedByPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(from, to, resultPromise, j);
                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (commonInvestedByPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [CIVBDepth], relations: [j], cost: commonInvestedByPathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            } else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestedByPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    // //单个企业直接投资关系路径查询
    // queryDirectInvestPath: async function (code, DIDepth, lowWeight, highWeight, lowFund, highFund) {
    //     return new Promise(async function (resolve, reject) {

    //         try {
    //             // let nodeId = await findNodeId(code);
    //             // let directInvestPathQuery = `start from=node(${nodeId}) match p= (from)-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             let directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             // let directInvestPathQuery = '';
    //             // if (!lowFund && !highFund) {
    //             //     directInvestPathQuery = `start from=node(${nodeId}) match p= (from)-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             // }
    //             // else if (lowFund && highFund) {
    //             //     directInvestPathQuery = `start from=node(${nodeId}) match p= (from)-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and to.RegFund >= ${lowFund} and to.RegFund <= ${highFund} and from.RegFund >= ${lowFund} and from.RegFund <= ${highFund}) return p`;
    //             // }
    //             // else if (lowFund && !highFund) {
    //             //     directInvestPathQuery = `start from=node(${nodeId}) match p= (from)-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and to.RegFund >= ${lowFund}) return p`;
    //             // }
    //             // else if (!lowFund && highFund) {
    //             //     directInvestPathQuery = `start from=node(${nodeId}) match p= (from)-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and to.RegFund <= ${highFund}) return p`;
    //             // }
    //             if (code == null) {
    //                 console.error(`${code} is not in the neo4j database at all !`);
    //                 logger.error(`${code} is not in the neo4j database at all !`);
    //                 return resolve({ error: `${code}=nodeId` });
    //             }
    //             else if (code != null) {
    //                 let now = 0;
    //                 now = Date.now();

    //                 //缓存
    //                 let resultPromise = null;
    //                 let previousValue = await cacheHandlers.getCache(directInvestPathQuery);
    //                 if (!previousValue) {
    //                     resultPromise = await session.run(directInvestPathQuery);
    //                     session.close();
    //                     cacheHandlers.setCache(directInvestPathQuery, resultPromise);
    //                 } else if (previousValue) {
    //                     resultPromise = previousValue;
    //                 }

    //                 // let resultPromise = await session.run(directInvestPathQuery);
    //                 let directInvestPathQueryCost = Date.now() - now;
    //                 logger.info("DirectInvestPathQueryCost: " + directInvestPathQueryCost + 'ms');
    //                 console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", DirectInvestPathQueryCost: " + directInvestPathQueryCost + 'ms');

    //                 let queryNodeResult = {};
    //                 let j = 6;                                                              //记录每种路径查询方式索引号,directInvestPathQuery索引为6
    //                 let handlerPromiseStart = Date.now();
    //                 let res = await handlerPromise(resultPromise, j);
    //                 let handlerPromiseCost = Date.now() - handlerPromiseStart;
    //                 console.log('DirectInvestPath_handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    //                 logger.info('DirectInvestPath_handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    //                 let beforePathDetail = res.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    //                 let newBeforePathDetail = [];
    //                 if (lowFund || highFund) {
    //                     let now = Date.now();
    //                     newBeforePathDetail = filterPathAccRegFund(beforePathDetail, lowFund, highFund);

    //                     let filterPathAccRegFundCost = Date.now() - now;
    //                     console.log('filterPathAccRegFundCost: ' + filterPathAccRegFundCost + 'ms');
    //                 } else if (!lowFund && !highFund) {
    //                     newBeforePathDetail = beforePathDetail;
    //                 }
    //                 let setSourceTargetStart = Date.now();
    //                 let afterPathDetail = setSourceTarget(res.mapRes, newBeforePathDetail);
    //                 let setSourceTargetCost = Date.now() - setSourceTargetStart;
    //                 console.log('DirectInvestPath_setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    //                 logger.info('DirectInvestPath_setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    //                 res.dataDetail.data.pathDetail = afterPathDetail;                       //用带ITName的pathDetail替换原来的
    //                 let newNames = [];
    //                 let nameSet = new Set();
    //                 for (let subPathDetail of afterPathDetail) {
    //                     for (let subPath of subPathDetail.path) {
    //                         nameSet.add(subPath.source);
    //                         nameSet.add(subPath.target);
    //                     }
    //                 }
    //                 for (let subName of nameSet) {
    //                     newNames.push(subName);
    //                 }
    //                 res.dataDetail.names = newNames;
    //                 res.dataDetail.data.pathNum = newBeforePathDetail.length;
    //                 // queryNodeResult.codes = res.uniqueCodes;
    //                 queryNodeResult.pathDetail = res.dataDetail;
    //                 // driver.close();
    //                 // session.close();
    //                 return resolve(queryNodeResult);
    //             }
    //         } catch (err) {
    //             console.error(err);
    //             logger.error(err);
    //             // session.close();
    //             // driver.close();
    //             return reject(err);
    //         }
    //     })
    //     // .timeout(lookupTimeout).catch( err => {
    //     //     console.log(err);
    //     //     logger.info(err);
    //     //     // return reject(err);
    //     // });  
    // },

    // //单个企业直接被投资关系路径查询
    // queryDirectInvestedByPath: async function (code, DIBDepth, lowWeight, highWeight, lowFund, highFund) {
    //     return new Promise(async function (resolve, reject) {

    //         try {
    //             // let nodeId = await findNodeId(code);
    //             // let directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             let directInvestedByPathQuery = `match p= (from:company{ITCode2: ${code}})<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             // let directInvestedByPathQuery = '';
    //             // if (!lowFund && !highFund) {
    //             //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
    //             // }
    //             // else if (lowFund && highFund) {
    //             //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and to.RegFund >= ${lowFund} and to.RegFund <= ${highFund}) return p`;
    //             // }
    //             // else if (lowFund && !highFund) {
    //             //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and to.RegFund >= ${lowFund}) return p`;
    //             // }
    //             // else if (!lowFund && highFund) {
    //             //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and to.RegFund <= ${highFund}) return p`;
    //             // }
    //             if (code == null) {
    //                 console.error(`${code} is not in the neo4j database at all !`);
    //                 logger.error(`${code} is not in the neo4j database at all !`);
    //                 return resolve({ error: `${code}=nodeId` });
    //             }
    //             else if (code != null) {
    //                 let now = 0;
    //                 now = Date.now();

    //                 //缓存
    //                 let resultPromise = null;
    //                 let previousValue = await cacheHandlers.getCache(directInvestedByPathQuery);
    //                 if (!previousValue) {
    //                     resultPromise = await session.run(directInvestedByPathQuery);
    //                     session.close();
    //                     cacheHandlers.setCache(directInvestedByPathQuery, resultPromise);
    //                 } else if (previousValue) {
    //                     resultPromise = previousValue;
    //                 }

    //                 // let resultPromise = await session.run(directInvestedByPathQuery);
    //                 let directInvestedByPathQueryCost = Date.now() - now;
    //                 logger.info("DirectInvestedByPathQueryCost: " + directInvestedByPathQueryCost + 'ms');
    //                 console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ", DirectInvestedByPathQueryCost: " + directInvestedByPathQueryCost + 'ms');

    //                 let queryNodeResult = {};
    //                 let j = 7;                  //记录每种路径查询方式索引号,directInvestedByPathQuery索引为7
    //                 let handlerPromiseStart = Date.now();
    //                 let res = await handlerPromise(resultPromise, j);
    //                 let handlerPromiseCost = Date.now() - handlerPromiseStart;
    //                 console.log('DirectInvestedByPath_handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    //                 logger.info('DirectInvestedByPath_handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    //                 let beforePathDetail = res.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    //                 let newBeforePathDetail = [];
    //                 if (lowFund || highFund) {
    //                     let now = Date.now();
    //                     newBeforePathDetail = filterPathAccRegFund(beforePathDetail, lowFund, highFund);

    //                     let filterPathAccRegFundCost = Date.now() - now;
    //                     console.log('filterPathAccRegFundCost: ' + filterPathAccRegFundCost + 'ms');
    //                 } else if (!lowFund && !highFund) {
    //                     newBeforePathDetail = beforePathDetail;
    //                 }
    //                 let setSourceTargetStart = Date.now();
    //                 let afterPathDetail = setSourceTarget(res.mapRes, newBeforePathDetail);
    //                 let setSourceTargetCost = Date.now() - setSourceTargetStart;
    //                 console.log('DirectInvestedByPath_setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    //                 logger.info('DirectInvestedByPath_setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    //                 res.dataDetail.data.pathDetail = afterPathDetail;                       //用带ITName的pathDetail替换原来的
    //                 let newNames = [];
    //                 let nameSet = new Set();
    //                 for (let subPathDetail of afterPathDetail) {
    //                     for (let subPath of subPathDetail.path) {
    //                         nameSet.add(subPath.source);
    //                         nameSet.add(subPath.target);
    //                     }
    //                 }
    //                 for (let subName of nameSet) {
    //                     newNames.push(subName);
    //                 }
    //                 res.dataDetail.names = newNames;
    //                 res.dataDetail.data.pathNum = newBeforePathDetail.length;
    //                 // queryNodeResult.codes = res.uniqueCodes;
    //                 queryNodeResult.pathDetail = res.dataDetail;
    //                 // driver.close();
    //                 // session.close();
    //                 return resolve(queryNodeResult);
    //             }
    //         } catch (err) {
    //             console.error(err);
    //             logger.error(err);
    //             // session.close();
    //             // driver.close();
    //             return reject(err);
    //         }
    //     })
    //     // .timeout(lookupTimeout).catch( err => {
    //     //     console.log(err);
    //     //     logger.info(err);
    //     //     // return reject(err);
    //     // });  
    // },

    //担保关系的路径查询
    queryGuaranteePath: async function (from, to, GTDepth) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 8;                                                                                   //记录每种路径查询方式索引号
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, GTDepth, j].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, GTDepth, 'GT'].join('-');
                if (!warmUpValue) {
                    // let nodeIdOne = await findNodeId(from);
                    // let nodeIdTwo = await findNodeId(to);
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'GuaranteePath', names: [], codes: [] } };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let fromOutBoundNum = await cacheHandlers.getCache(`${from}-OutBound`);
                        if (!fromOutBoundNum) {
                            let fromOutBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    fromOutBoundNum = await getGuaranteeOutBoundNodeNum(from, GTDepth);                                   //获取from的outbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' +from);
                                logger.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' +from);
                            }
                            let fromOutBoundNumCost = Date.now() - fromOutBoundNumStart;
                            console.log(`${from} fromOutBoundNumCost: ` + fromOutBoundNumCost + 'ms' + ', fromOutBoundNum: ' + fromOutBoundNum);
                            cacheHandlers.setCache(`${from}-OutBound`, fromOutBoundNum);
                        }
                        let toInBoundNum = await cacheHandlers.getCache(`${to}-InBound`);
                        if (!toInBoundNum) {
                            let toInBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    toInBoundNum = await getGuaranteeInBoundNodeNum(to, GTDepth);                                    //获取to的inbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' +to);
                                logger.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' +to);
                            }
                            let toInBoundNumCost = Date.now() - toInBoundNumStart;
                            console.log(`${to} toInBoundNumCost: ` + toInBoundNumCost + 'ms' + ', toInBoundNum: ' + toInBoundNum);
                            cacheHandlers.setCache(`${to}-InBound`, toInBoundNum);
                        }
                        let guaranteePathQuery = null;
                        let guaranteePathQuery_outBound = `match p= (from:company{ITCode2: ${from}})-[r:guarantees*..${GTDepth}]->(to:company{ITCode2: ${to}}) return p`;
                        let guaranteePathQuery_inBound = `match p= (from:company{ITCode2: ${to}})<-[r:guarantees*..${GTDepth}]-(to:company{ITCode2: ${from}}) return p`;
                        if (fromOutBoundNum > toInBoundNum) {
                            guaranteePathQuery = guaranteePathQuery_outBound;
                        } else if (fromOutBoundNum <= toInBoundNum) {
                            guaranteePathQuery = guaranteePathQuery_inBound;
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let resultPromise = null;
                            now = Date.now();
                            // resultPromise = await session.run(guaranteePathQuery);

                            let retryCount = 0;
                            do {
                                try {
                                    resultPromise = await sessionRun7(guaranteePathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun7 execute fail after trying 3 times: ' +guaranteePathQuery);
                                logger.error('sessionRun7 execute fail after trying 3 times: ' +guaranteePathQuery);
                            } 
                            console.log('query neo4j server: ' +guaranteePathQuery);
                            logger.info('query neo4j server: ' +guaranteePathQuery);
                            let guaranteePathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " guaranteePathQueryCost: " + guaranteePathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `from: ${from} to: ${to}` + ", guaranteePathQueryCost: " + guaranteePathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(resultPromise, j);
                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (guaranteePathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [GTDepth], relations: [j], cost: guaranteePathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'GuaranteePath', names: [], codes: [] } };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

    //直接被投资关系的路径查询
    queryGuaranteedByPath: async function (from, to, GTBDepth) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 9;                                                            //记录每种路径查询方式索引号
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, GTBDepth, j].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, GTBDepth, 'GTB'].join('-');
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'GuaranteedByPath', names: [], codes: [] } };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let fromInBoundNum = await cacheHandlers.getCache(`${from}-InBound`);
                        if (!fromInBoundNum) {
                            let fromInBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    fromInBoundNum = await getGuaranteeInBoundNodeNum(from, GTBDepth);                                     //获取from的inbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' +from);
                                logger.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' +from);
                            }
                            let fromInBoundNumCost = Date.now() - fromInBoundNumStart;
                            console.log(`${from} fromInBoundNumCost: ` + fromInBoundNumCost + 'ms' + ', fromInBoundNum: ' + fromInBoundNum);
                            cacheHandlers.setCache(`${from}-InBound`, fromInBoundNum);
                        }
                        let toOutBoundNum = await cacheHandlers.getCache(`${to}-OutBound`);
                        if (!toOutBoundNum) {
                            let toOutBoundNumStart = Date.now();
                            let retryCount = 0;
                            do {
                                try {
                                    toOutBoundNum = await getGuaranteeOutBoundNodeNum(to, GTBDepth);                                     //获取to的outbound节点数
                                    break;
                                } catch (err) {
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' +to);
                                logger.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' +to);
                            }
                            let toOutBoundNumCost = Date.now() - toOutBoundNumStart;
                            console.log(`${to} toOutBoundNumCost: ` + toOutBoundNumCost + 'ms' + ', toOutBoundNum: ' + toOutBoundNum);
                            cacheHandlers.setCache(`${to}-OutBound`, toOutBoundNum);
                        }
                        let guaranteedByPathQuery = null;
                        let guaranteedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:guarantees*..${GTBDepth}]-(to:company{ITCode2: ${to}}) return p`;
                        let guaranteedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:guarantees*..${GTBDepth}]->(to:company{ITCode2: ${from}}) return p`;
                        if (toOutBoundNum < fromInBoundNum) {
                            guaranteedByPathQuery = guaranteedByPathQuery_inBound;
                        } else if (toOutBoundNum >= fromInBoundNum) {
                            guaranteedByPathQuery = guaranteedByPathQuery_outBound;
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let resultPromise = null;
                            now = Date.now();
                            // resultPromise = await session.run(guaranteedByPathQuery);

                            let retryCount = 0;
                            do {
                                try {
                                    resultPromise = await sessionRun8(guaranteedByPathQuery);
                                    break;
                                } catch (err) {
                                    retryCount++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCount < 3)
                            if (retryCount == 3) {
                                console.error('sessionRun8 execute fail after trying 3 times: ' +guaranteedByPathQuery);
                                logger.error('sessionRun8 execute fail after trying 3 times: ' +guaranteedByPathQuery);
                            } 
                            console.log('query neo4j server: ' +guaranteedByPathQuery);
                            logger.info('query neo4j server: ' +guaranteedByPathQuery);
                            let guaranteedByPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " guaranteedByPathQueryCost: " + guaranteedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `from: ${from} to: ${to}` + ", guaranteedByPathQueryCost: " + guaranteedByPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(resultPromise, j);
                                cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                //根据预热条件的阈值判断是否要加入预热
                                let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                if (guaranteedByPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                    let conditionsKey = config.redisKeyName.conditionsKey;
                                    let conditionsField = [from, to].join('->');
                                    let conditionsValue = { from: from, to: to, depth: [GTBDepth], relations: [j], cost: guaranteedByPathQueryCost };
                                    cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                    cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'guaranteedByPath', names: [], codes: [] } };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                // session.close();
                // driver.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });  
    },

}

module.exports = searchGraph;