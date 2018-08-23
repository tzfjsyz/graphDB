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
        let resultPromise = session.run(`match (compId:company {ITCode2: '${code}'}) return compId`);
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
        // let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})-[r:invests*..${depth}]->(to) return count(to)`);
        let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})-[r:invests*..${depth}]->() return count(r)`);
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
        // let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})<-[r:invests*..${depth}]-(to) return count(to)`);
        let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})<-[r:invests*..${depth}]-() return count(r)`);
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
        // let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})-[r:guarantees*..${depth}]->(to) return count(to)`);
        let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})-[r:guarantees*..${depth}]->() return count(r)`);
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
        // let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})<-[r:guarantees*..${depth}]-(to) return count(to)`);
        let resultPromise = session.run(`match (from:company{ITCode2: '${code}'})<-[r:guarantees*..${depth}]-() return count(r)`);
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
    let result = { sourceIsRepeat: false, targetIsRepeat: false };
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
    let result = { flag: true, pathDetail: [] };
    let isFromToDirection = 1; 
    let pathLength = pathArray.length;
    let newPathArray = [];
    if (pathLength > 1) {
        if ( (pathArray[0].targetCode == to && pathArray[0].sourceCode != from) || (pathArray[0].targetCode == from && pathArray[0].sourceCode != to) ){
            isFromToDirection = 0
        }
        //如果from<-to方向的path需倒序排列
        if (isFromToDirection == 0) {
            for (let i = pathLength - 1; i >= 0; i--) {
                newPathArray.push(pathArray[i]);
            }
            pathArray = newPathArray;
        }
        for (let i = 0; i < pathLength - 1; i++) {
            let nextSourceCode = pathArray[i + 1].sourceCode;
            let targetCode = pathArray[i].targetCode;
            let sourceCode = pathArray[i].sourceCode;
            let nextTargetCode = pathArray[i + 1].targetCode;
            if (targetCode != nextSourceCode || sourceCode == nextTargetCode || targetCode == from || targetCode == to) {                     //targetCode != nextSourceCode:不连续的path;  sourceCode == nextTargetCode：闭环的path
                console.log('该invest path不符合要求！');
                result.flag = false;
                break;
            }
        }

        // for (let i = pathLength - 1; i >= 1; i--) {
        //     let nextSourceCode = pathArray[i - 1].sourceCode;
        //     let targetCode = pathArray[i].targetCode;
        //     let sourceCode = pathArray[i].sourceCode;
        //     let nextTargetCode = pathArray[i - 1].targetCode;
        //     if (targetCode != nextSourceCode || sourceCode == nextTargetCode || targetCode == from || targetCode == to) {                     //targetCode != nextSourceCode:不连续的path;  sourceCode == nextTargetCode：闭环的path
        //         console.log('该invest path不符合要求！');
        //         flag = false;
        //         break;
        //     }
        // }
    }
    result.pathDetail = pathArray;
    return result;
}

//判断invest path的是否闭环
function isClosedLoopPath(from, to, pathArray) {
    // from = parseInt(from);
    // to = parseInt(to);
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
    //判断from、to是否自然人的personalCode
    let fromIsPerson = 0;
    let toIsPerson = 0;
    if (null != from && from.indexOf('P') >= 0) {
        fromIsPerson = 1;
    }
    if (null != to && to.indexOf('P') >= 0) {
        toIsPerson = 1;
    }

    let allNamesOne = [];
    let allCodesOne = new Set();                                       //使用set保证数据的唯一性
    let newAllCodesOne = new Set();                                       //含自然人
    let newCodesOne = new Set();                                       //不含自然人
    let uniqueCodesOne = [];                                           //存储唯一性的codes数据元素
    // let num = 0;

    let allNamesTwo = [];
    let allCodesTwo = new Set();                                       //使用set保证数据的唯一性
    let uniqueCodesTwo = [];                                           //存储唯一性的codes数据元素

    let uniqueCodesThree = new Set();
    let uniqueNamesThree = new Set();
    let uniqueCodesThreeArray = [];
    let uniqueNamesThreeArray = [];

    let allNamesFour = [];
    let uniqueCodesFour = new Set();
    let uniqueNamesFour = new Set();
    let uniqueCodesFourArray = [];
    let uniqueNamesFourArray = [];

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
    let promiseResultThree = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [] } };
    let promiseResultFour = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_12`, typeName: 'executiveInvestPath', names: [] } };
    // promiseResult.dataDetail.data.pathNum = result.records.length;
    // num += result.records.length;                                  //调用 neo4j 返回的所有路径，包括有重复点的路径

    if (result.records.length == 0) {
        promiseResult.pathTypeOne = promiseResultOne;
        promiseResult.pathTypeTwo = promiseResultTwo;
        promiseResult.pathTypeThree = promiseResultThree;
        promiseResult.pathTypeFour = promiseResultFour;
        return promiseResult;
    }
    else if (result.records.length > 0) {
        let tempResultTwo = [];                                                                          //临时存放所有的担保关系路径
        let newTempResultTwo = [];                                                                       //筛选后的担保关系路径
        let tempResultThree = [];                                                                        //存放家族关系路径
        let newTempResultThree = [];

        for (let subRecord of result.records) {
            // let pathArrayOne = {};
            // let tempPathArrayOne = [];
            // let pathArrayTwo = {};
            // let tempPathArrayTwo = [];
            // let pathArrayThree = {};
            // let tempPathArrayThree = [];
            // let pathArrayFour = {};
            // let tempPathArrayFour = [];
            let pathOneCodeSet = new Set();                                                              //存放pathOne中所有的ITCode2(除自然人)

            for (let subField of subRecord._fields) {
                let pathArrayOne = {};
                let pathArrayTwo = {};
                let pathArrayThree = {};
                let pathArrayFour = {};

                let tempPathArrayOne = [];
                let tempPathArrayTwo = [];
                let tempPathArrayThree = [];
                let tempPathArrayFour = [];

                if (null != subField) {
                    for (let subSegment of subField.segments) {

                        //取到start nodes的isPerson属性
                        let startIsPerson = null;
                        if (null != subSegment.start.properties.isPerson) {
                            startIsPerson = subSegment.start.properties.isPerson;
                        }
                        else if (null == subSegment.start.properties.isPerson && null != subSegment.start.properties.isPerson.low) {
                            startIsPerson = subSegment.start.properties.isPerson.low;
                        }
    
                        //取到end nodes的isPerson属性
                        let endIsPerson = null;
                        if (null != subSegment.end.properties.isPerson) {
                            endIsPerson = subSegment.end.properties.isPerson;
                        }
                        else if (null == subSegment.end.properties.isPerson && null != subSegment.end.properties.isPerson.low) {
                            endIsPerson = subSegment.end.properties.isPerson.low;
                        }
    
                        //通过isPerson为ITCode2取值
                        let startSegmentCode = null;
                        let endSegmentCode = null;
                        // if (startIsPerson == '1' || startIsPerson == 1) {
                        //     let id = subSegment.start.properties.ITCode2.low;
                        //     startSegmentCode = 'P' + pad(id, 9);
                        // } else {
                        //     startSegmentCode = subSegment.start.properties.ITCode2.low;
                        // }
                        // if (endIsPerson == '1' || endIsPerson == 1) {
                        //     let id = subSegment.end.properties.ITCode2.low;
                        //     endSegmentCode = 'P' + pad(id, 9);
                        // } else {
                        //     endSegmentCode = subSegment.end.properties.ITCode2.low;
                        // }
                        if (null != subSegment.start.properties.ITCode2) {
                            startSegmentCode = subSegment.start.properties.ITCode2;
                        }
                        else if (null == subSegment.start.properties.ITCode2 && null != subSegment.start.properties.ITCode2.low) {
                            startSegmentCode = subSegment.start.properties.ITCode2.low;
                        }
                        if (null != subSegment.end.properties.ITCode2) {
                            endSegmentCode = subSegment.end.properties.ITCode2;
                        }
                        else if (null == subSegment.end.properties.ITCode2 && null != subSegment.end.properties.ITCode2.low) {
                            endSegmentCode = subSegment.end.properties.ITCode2.low;
                        }
    
                        // allNames.push(subSegment.start.properties.ITName, subSegment.end.properties.ITName);
                        // allCodes.push(subSegment.start.properties.ITCode2, subSegment.end.properties.ITCode2);
                        let startSegmentLow = subSegment.start.identity.low;
                        let relSegmentStartLow = subSegment.relationship.start.low;
                        let startRegFund = 0;                                                                  //注册资金
                        if (startIsPerson == 0 || startIsPerson == '0') {
                            if (null != subSegment.start.properties.RMBFund) {
                                startRegFund = subSegment.start.properties.RMBFund;
                            }
                            else if (null == subSegment.start.properties.RMBFund && null != subSegment.start.properties.RMBFund.low) {
                                startRegFund = subSegment.start.properties.RMBFund.low;
                            }
                            startRegFund = parseFloat(startRegFund.toFixed(2));                            //将RegFund值转换2位小数              
                        }
    
                        let relSegmentEndLow = subSegment.relationship.end.low;
    
                        //获取每个path relationship type
                        let relationshipType = null;
                        if (Object.keys(subSegment.relationship.type).length > 0) {
                            if (null != subSegment.relationship.type) {
                                relationshipType = subSegment.relationship.type;
                            }
                            else if (null == subSegment.relationship.type && null != subSegment.relationship.type.low) {
                                relationshipType = subSegment.relationship.type.low;
                            }
                        }
                        //如果weight是全量导入的，直接取weight的值；如果weight是增量导入的，需要取到weight节点下的low值
                        let weight = 0;                                                                    //持股比例
                        let relCode = 0;                                                                   //家族关系代码
                        let relName = null;                                                                //家族关系名称
                        if (Object.keys(subSegment.relationship.properties).length > 0) {
                            if (relationshipType != 'family') {
                                if (null != subSegment.relationship.properties.weight) {
                                    weight = subSegment.relationship.properties.weight;
                                }
                                else if (null == subSegment.relationship.properties.weight && null != subSegment.relationship.properties.weight.low) {
                                    weight = subSegment.relationship.properties.weight.low;
                                }
                                weight = parseFloat(weight.toFixed(2));                                        //将weight值转换2位小数
                            }
                            else if (relationshipType == 'family') {
                                if (null != subSegment.relationship.properties.relationCode.low) {
                                    relCode = subSegment.relationship.properties.relationCode.low;
                                }
                                else if (null == subSegment.relationship.properties.relationCode.low && null != subSegment.relationship.properties.relationCode) {
                                    relCode = subSegment.relationship.properties.relationCode;
                                }
                                if (null != subSegment.relationship.properties.relationName) {
                                    relName = subSegment.relationship.properties.relationName;
                                }
                                else if (null == subSegment.relationship.properties.relationName && null != subSegment.relationship.properties.relationName.low) {
                                    relName = subSegment.relationship.properties.relationName.low;
                                }
                            }
                        }
    
                        let endSegmentLow = subSegment.end.identity.low;
                        // let endSegmentName = subSegment.end.properties.ITName;
                        // let endSegmentCode = subSegment.end.properties.ITCode2.low;
                        // let endSegmentCode = subSegment.end.properties.ITCode2;
                        let endRegFund = 0;                                                               //注册资金
                        if (endIsPerson == 0 || endIsPerson == '0') {
                            if (null != subSegment.end.properties.RMBFund) {
                                endRegFund = subSegment.end.properties.RMBFund;
                            }
                            else if (null == subSegment.end.properties.RMBFund && null != subSegment.end.properties.RMBFund.low) {
                                endRegFund = subSegment.start.properties.RMBFund.low;
                            }
                            endRegFund = parseFloat(endRegFund.toFixed(2));                               //将RegFund值转换2位小数    
                        }
    
                        // let startSegmentName = await queryCodeToName(startSegmentCode);                 //不直接获取eno4j中的ITName，而是通过外部接口由ITCode2获取ITName
                        // let endSegmentName = await queryCodeToName(endSegmentCode);
                        // allNames.push(startSegmentName, endSegmentName);
                        // allCodes.push(`${startSegmentCode}`, `${endSegmentCode}`);
    
                        //处理无机构代码的start nodes的name问题
                        let startSegmentName = null;
                        //取到start nodes的isExtra属性
                        let startIsExtra = null;
                        if (null != subSegment.start.properties.isExtra) {
                            startIsExtra = subSegment.start.properties.isExtra;
                        }
                        else if (null == subSegment.start.properties.isExtra && null != subSegment.start.properties.isExtra.low) {
                            startIsExtra = subSegment.start.properties.isExtra.low;
                        }
                        if (startIsExtra == 1 || startIsExtra == '1' || startIsExtra == 'null') {
                            startSegmentName = subSegment.start.properties.name;
                        }
                        else if (startIsExtra == 0 || startIsExtra == '0') {
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
    
                        //处理无机构代码的end nodes的name问题
                        let endSegmentName = null;
                        //取到end nodes的isExtra属性
                        let endIsExtra = null;
                        if (null != subSegment.end.properties.isExtra) {
                            endIsExtra = subSegment.end.properties.isExtra;
                        }
                        else if (null == subSegment.end.properties.isExtra && null != subSegment.end.properties.isExtra.low) {
                            endIsExtra = subSegment.end.properties.isExtra.low;
                        }
                        if (endIsExtra == 1 || endIsExtra == '1' || endIsExtra == 'null') {
                            endSegmentName = subSegment.end.properties.name;
                        }
                        else if (endIsExtra == 0 || endIsExtra == '0') {
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
    
                        let pathOne = {                                                                               //invests的path
                            source: null, sourceCode: null, sourceRegFund: null, sourceIsExtra: null, sourceIsPerson: null,
                            target: null, targetCode: null, targetRegFund: null, targetIsExtra: null, targetIsPerson: null, weight: null
                        };
                        let pathTwo = {                                                                               //guarantees的path
                            source: null, sourceCode: null, sourceRegFund: null, sourceIsExtra: null, sourceIsPerson: null,
                            target: null, targetCode: null, targetRegFund: null, targetIsExtra: null, targetIsPerson: null, weight: null
                        };
                        let pathThree = {                                                                             //family的path
                            source: null, sourceCode: null, sourceIsExtra: null, sourceIsPerson: null,
                            target: null, targetCode: null, targetIsExtra: null, targetIsPerson: null, relationCode: null, relationName: null
                        };
                        let pathFour = {                                                                             //executes的path
                            compCode: null, compName: null, RMBRegFund: null, isExtra: null, isPerson: null
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
                        else if (relationshipType == 'family') {
                            if (relSegmentStartLow == startSegmentLow && relSegmentEndLow == endSegmentLow) {
                                pathThree.source = startSegmentName;
                                pathThree.sourceCode = startSegmentCode;
                                pathThree.sourceIsExtra = startIsExtra;
                                pathThree.sourceIsPerson = startIsPerson;
                                pathThree.target = endSegmentName;
                                pathThree.targetCode = endSegmentCode;
                                pathThree.targetIsExtra = endIsExtra;
                                pathThree.targetIsPerson = endIsPerson;
                                pathThree.relationCode = relCode;
                                pathThree.relationName = relName;
                                // pathTwo.pathType = relationshipType;
                                // uniqueCodesThree.add(startSegmentCode);
                                // uniqueCodesThree.add(endSegmentCode);
                                // uniqueNamesThree.add(startSegmentName);
                                // uniqueNamesThree.add(endSegmentName);
    
                            }
                            else if (relSegmentStartLow == endSegmentLow && relSegmentEndLow == startSegmentLow) {
                                pathThree.source = endSegmentName;
                                pathThree.sourceCode = endSegmentCode;
                                pathThree.sourceIsExtra = endIsExtra;
                                pathThree.sourceIsPerson = endIsPerson;
                                pathThree.target = startSegmentName;
                                pathThree.targetCode = startSegmentCode;
                                pathThree.targetIsExtra = startIsExtra;
                                pathThree.targetIsPerson = startIsPerson;
                                pathThree.relationCode = relCode;
                                pathThree.relationName = relName;
                                // pathTwo.pathType = relationshipType;
                                // uniqueCodesThree.add(startSegmentCode);
                                // uniqueCodesThree.add(endSegmentCode);
                                // uniqueNamesThree.add(startSegmentName);
                                // uniqueNamesThree.add(endSegmentName);
    
                            }
                        }
                        else if (relationshipType == 'executes') {
                            pathFour.compCode = endSegmentCode;
                            if (endIsExtra == 1 || endIsExtra == '1') {
                                pathFour.compName = endSegmentName;
                                uniqueNamesFour.add(endSegmentName);
                            }
                            else {
                                uniqueCodesFour.add(endSegmentCode);
                            }
                            pathFour.RMBRegFund = endRegFund;
                            pathFour.isExtra = endIsExtra;
                            pathFour.isPerson = endIsPerson;
    
                        }
    
                        if (null != pathOne.sourceCode && null != pathOne.targetCode) {
                            // delete pathOne.pathType;                                             //删除pathType属性
                            tempPathArrayOne.push(pathOne);
                        }
                        //invests关系存在的前提下, 添加guarantees关系的path
                        if (null != pathTwo.sourceCode && null != pathTwo.targetCode && pathTwo.sourceCode != pathTwo.targetCode) {
                            // delete pathTwo.pathType;
                            tempPathArrayTwo.push(pathTwo);
                        }
                        if (null != pathThree.sourceCode && null != pathThree.targetCode) {
                            tempPathArrayThree.push(pathThree);                                        //存放家族关系路径
                        }
                        if (null != pathFour.compCode) {
                            tempPathArrayFour.push(pathFour);
                        }
    
                    }
    
                    //处理投资关系--pathArrayOne
                    if (null != from && null != to && tempPathArrayOne.length > 0) {
                        //判断form/to中是否有自然人
                        if (fromIsPerson == 1 || toIsPerson == 1) {
                            pathArrayOne.path = tempPathArrayOne;
                            if (fromIsPerson == 1 && toIsPerson == 0) {
                                let sourceCodeOne = tempPathArrayOne[0].sourceCode;
                                if (sourceCodeOne == from) {
                                    pathArrayOne.isMainPath = 1;
                                }
                                else if (sourceCodeOne != from) {
                                    pathArrayOne.isMainPath = 0;
                                }
                            }
                            else if (toIsPerson == 1 && fromIsPerson == 0) {
                                let sourceCodeOne = tempPathArrayOne[0].sourceCode;
                                if (sourceCodeOne == to) {
                                    pathArrayOne.isMainPath = 1;
                                }
                                else if (sourceCodeOne != to) {
                                    pathArrayOne.isMainPath = 0;
                                }
                            }
                            else {
                                pathArrayOne.isMainPath = 1;
                            }
                        }
                        else if (fromIsPerson == 0 && toIsPerson == 0) {
                            //如果from和to任何一个不在pathOne中，则剔除这条path
                            if (pathOneCodeSet.has(from) && pathOneCodeSet.has(to)) {
                                pathArrayOne.path = tempPathArrayOne;
                                pathArrayOne.isMainPath = 1;
                                //将每个path下的pathOneCodeSet中的元素添加到allCodesOne中
                                allCodesOne = new Set([...allCodesOne, ...pathOneCodeSet]);
                            }
                        }
                        //处理投资关系--pathArrayOne
                        if (pathArrayOne.hasOwnProperty('path') && pathArrayOne.path.length > 0) {
                            // promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                            //过滤重复的path
                            let pathGroup = promiseResultOne.dataDetail.data.pathDetail;
                            let isDupFlag = isDuplicatedPath(pathGroup, pathArrayOne);
                            if (isDupFlag == false) {
    
                                //判断form/to都不是自然人
                                // if (from.indexOf('P') < 0 && to.indexOf('P') < 0) {
                                //处理共同投资和共同股东关系路径
                                if (index == 4 || index == 5) {
                                    let sourceCodeVal = pathArrayOne.path[0].sourceCode;
                                    let targetCodeVal = pathArrayOne.path[0].targetCode;
                                    let fromCode = from;
                                    let toCode = to;
                                    let result = findCodeAppearTimes(pathArrayOne.path);
                                    //如果source/target都出现2次以上，过滤掉
                                    if (result.sourceIsRepeat == true && result.targetIsRepeat == true) {
                                        console.log('过滤1条出现重复节点的path!');
                                    }
                                    else if (result.sourceIsRepeat == false || result.targetIsRepeat == false) {
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
                                                            newCodesOne.add(`${ITCode_source}`);
                                                        }
                                                        if (isPerson_target != 1 && isPerson_target != '1') {
                                                            newCodesOne.add(`${ITCode_target}`);
                                                        }
                                                        newAllCodesOne.add(ITCode_source);
                                                        newAllCodesOne.add(ITCode_target);
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
                                                            newCodesOne.add(`${ITCode_source}`);
                                                        }
                                                        if (isPerson_target != 1 && isPerson_target != '1') {
                                                            newCodesOne.add(`${ITCode_target}`);
                                                        }
                                                        newAllCodesOne.add(ITCode_source);
                                                        newAllCodesOne.add(ITCode_target);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                //处理直接投资关系路径
                                else if (index == 0) {
                                    let flag = false;
                                    // if (fromIsPerson == 1 || toIsPerson == 1) {
                                    //     flag = true;
                                    // }
                                    // else {
                                    //     let result = isContinuousPath(from, to, pathArrayOne.path);
                                    //     flag = result.flag;
                                    //     pathArrayOne.path = result.pathDetail;
                                    // }
                                    let result = isContinuousPath(from, to, pathArrayOne.path);
                                    flag = result.flag;
                                    pathArrayOne.path = result.pathDetail;
                                    if (flag == true) {
                                        promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                        for (let subPath of pathArrayOne.path) {
                                            let isPerson_source = subPath.sourceIsPerson;
                                            let isPerson_target = subPath.targetIsPerson;
                                            let ITCode_source = subPath.sourceCode;
                                            let ITCode_target = subPath.targetCode;
                                            if (isPerson_source != 1 && isPerson_source != '1') {
                                                newCodesOne.add(`${ITCode_source}`);
                                            }
                                            if (isPerson_target != 1 && isPerson_target != '1') {
                                                newCodesOne.add(`${ITCode_target}`);
                                            }
                                            newAllCodesOne.add(ITCode_source);
                                            newAllCodesOne.add(ITCode_target);
                                        }
                                    }
                                }
                                //处理直接被投资关系路径
                                else if (index == 1) {
                                    let flag = false;
                                    // if (fromIsPerson == 1 || toIsPerson == 1) {
                                    //     flag = true;
                                    // }
                                    // else {
                                    //     let result = isContinuousPath(from, to, pathArrayOne.path);
                                    //     flag = result.flag;
                                    //     pathArrayOne.path = result.pathDetail;
                                    // }
                                    let result = isContinuousPath(from, to, pathArrayOne.path);
                                    flag = result.flag;
                                    pathArrayOne.path = result.pathDetail;
                                    if (flag == true) {
                                        promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                        for (let subPath of pathArrayOne.path) {
                                            let isPerson_source = subPath.sourceIsPerson; 7
                                            let isPerson_target = subPath.targetIsPerson;
                                            let ITCode_source = subPath.sourceCode;
                                            let ITCode_target = subPath.targetCode;
                                            if (isPerson_source != 1 && isPerson_source != '1') {
                                                newCodesOne.add(`${ITCode_source}`);
                                            }
                                            if (isPerson_target != 1 && isPerson_target != '1') {
                                                newCodesOne.add(`${ITCode_target}`);
                                            }
                                            newAllCodesOne.add(ITCode_source);
                                            newAllCodesOne.add(ITCode_target);
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
                                            newCodesOne.add(`${ITCode_source}`);
                                        }
                                        if (isPerson_target != 1 && isPerson_target != '1') {
                                            newCodesOne.add(`${ITCode_target}`);
                                        }
                                        newAllCodesOne.add(ITCode_source);
                                        newAllCodesOne.add(ITCode_target);
                                    }
                                }
                            }
                        }
                    }
    
                    pathArrayTwo.path = tempPathArrayTwo;
                    if (tempPathArrayThree.length > 0) {
                        // pathArrayThree.path = [tempPathArrayThree[0]];                                  //family关系只取一层关系
                        pathArrayThree.path = tempPathArrayThree;
                    }
    
                    //处理担保关系--pathArrayTwo
                    if (pathArrayTwo.hasOwnProperty('path') && pathArrayTwo.path.length > 0) {
                        tempResultTwo.push(pathArrayTwo);
                    }
    
                    //处理家族关系--pathArrayThree
                    if (pathArrayThree.hasOwnProperty('path') && pathArrayThree.path.length > 0) {
                        //过滤重复path
                        let pathGroup = [];
                        if (tempResultThree.length > 0) {
                            for (let subResultThree of tempResultThree) {
                                pathGroup.push(subResultThree);
                            }
                        }
                        if (pathGroup.length > 0) {
                            let isDupFlag = isDuplicatedPath(pathGroup, pathArrayThree);
                            if (isDupFlag == false) {
                                tempResultThree.push(pathArrayThree);
                            }
                        }
                        else if (pathGroup.length == 0) {
                            tempResultThree.push(pathArrayThree);
                        }
                    }
    
                    //处理高管投资关系--pathArrayFour
                    pathArrayFour.path = tempPathArrayFour;
                    promiseResultFour.dataDetail.data.pathDetail.push(pathArrayFour);                   
                }
               

            }

        }

        uniqueCodesOne = Array.from(newCodesOne);
        // uniqueCodesThreeArray = Array.from(uniqueCodesThree);
        // uniqueNamesThreeArray = Array.from(uniqueNamesThree);
        uniqueCodesFourArray = Array.from(uniqueCodesFour);
        uniqueNamesFourArray = Array.from(uniqueNamesFour);

        //根据投资关系pathArrayOne中涉及到的机构才展示担保关系pathArrayTwo
        for (let subResult of tempResultTwo) {
            let newSubResult = { path: [] };
            for (let subPathDetail of subResult.path) {
                let sourceCode_two = subPathDetail.sourceCode;
                let targetCode_two = subPathDetail.targetCode;
                if (newCodesOne.has(`${sourceCode_two}`) == true && newCodesOne.has(`${targetCode_two}`) == true) {
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

        //根据投资关系pathArrayOne中涉及到的机构才展示家族关系pathArrayThree
        for (let subResult of tempResultThree) {
            let newSubResult = { path: [] };
            for (let subPathDetail of subResult.path) {
                let sourceCode_three = subPathDetail.sourceCode;
                let targetCode_three = subPathDetail.targetCode;
                //直接投资关系(被投资关系)时，from/to有自然人，不过滤
                if ((index == 0 || index == 1) && (fromIsPerson == 1 || toIsPerson == 1)) {
                    newSubResult.path.push(subPathDetail);
                }
                else {
                    if (newAllCodesOne.has(`${sourceCode_three}`) == true && newAllCodesOne.has(`${targetCode_three}`) == true) {
                        newSubResult.path.push(subPathDetail);
                    }
                }
            }
            if (newSubResult.path.length > 0) {
                newTempResultThree.push(newSubResult);
                //过滤重复的path
                let pathGroup = promiseResultThree.dataDetail.data.pathDetail;
                let isDupFlag = isDuplicatedPath(pathGroup, newSubResult);
                if (isDupFlag == false) {
                    promiseResultThree.dataDetail.data.pathDetail.push(newSubResult);
                    for (let newSubPathDetail of newSubResult.path) {
                        uniqueCodesThree.add(newSubPathDetail.sourceCode);
                        uniqueCodesThree.add(newSubPathDetail.targetCode);
                        uniqueNamesThree.add(newSubPathDetail.source);
                        uniqueNamesThree.add(newSubPathDetail.target);
                    }
                }
            }
        }

        uniqueCodesThreeArray = Array.from(uniqueCodesThree);
        uniqueNamesThreeArray = Array.from(uniqueNamesThree);

        let retryCount = 0;
        do {
            try {
                if (uniqueCodesOne.length > 0) {
                    allNamesOne = await cacheHandlers.getAllNames(uniqueCodesOne);             //ITCode2->ITName
                }
                if (uniqueCodesTwo.length > 0) {
                    allNamesTwo = await cacheHandlers.getAllNames(uniqueCodesTwo);             //ITCode2->ITName
                }
                if (uniqueCodesFourArray.length > 0) {
                    allNamesFour = await cacheHandlers.getAllNames(uniqueCodesFourArray);
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

        let newAllNamesFour = handlerAllNames(allNamesFour);
        let codeNameMapResFour = getCodeNameMapping(uniqueCodesFourArray, newAllNamesFour);

        promiseResultThree.dataDetail.names = uniqueNamesThreeArray;
        promiseResultThree.dataDetail.codes = uniqueCodesThreeArray;
        // promiseResultThree.dataDetail.data.pathDetail = tempResultThree;
        promiseResultThree.dataDetail.data.pathNum = promiseResultThree.dataDetail.data.pathDetail.length;

        promiseResultFour.mapRes = codeNameMapResFour;
        promiseResultFour.dataDetail.names = uniqueNamesFourArray.concat(newAllNamesFour);
        promiseResultFour.dataDetail.codes = uniqueCodesFourArray;
        promiseResultFour.dataDetail.data.pathNum = promiseResultFour.dataDetail.data.pathDetail.length;

        promiseResult.pathTypeOne = promiseResultOne;
        promiseResult.pathTypeTwo = promiseResultTwo;
        promiseResult.pathTypeThree = promiseResultThree;
        promiseResult.pathTypeFour = promiseResultFour;

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

//为每个path追加增加ITName属性
function setSourceTarget2(map, pathDetail) {
    // let newPathDetail = [];
    try {
        for (let subPathDetail of pathDetail) {
            for (let subPath of subPathDetail.path) {
                let compName = null;
                if (subPath.isExtra == '1' || subPath.isExtra == 1 || subPath.isExtra == 'null') {
                    compName = subPath.compName;
                }
                else if (subPath.isExtra == '0' || subPath.isExtra == 0) {
                    compName = map.get(`${subPath.compCode}`);
                }
                subPath.compName = compName;
            }
        }
        return pathDetail;
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }

}

//按 注册资本, 名称因子排序
function sortRegName(a, b) {
    let compName_a = a.path[0].compName;
    let RMBRegFund_a = a.path[0].RMBRegFund;
    let compName_b = b.path[0].compName;
    let RMBRegFund_b = b.path[0].RMBRegFund;
    let flag = 0;
    let flagOne = RMBRegFund_b - RMBRegFund_a;                                                                  //注册资本 降序
    let flagTwo = compName_b - compName_a;
    //1. 注册资本 = 0
    if (flagOne == 0) {
        flag = flagTwo;                                                                                          //名称 降序
    }
    else if (flagOne != 0) {
        flag = flagOne;
    }
    return flag;
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
    let queryNodeResult = { nodeResultOne: { pathDetail: {} }, nodeResultTwo: { pathDetail: {} }, nodeResultThree: { pathDetail: {} } };
    let handlerPromiseStart = Date.now();
    let promiseResult = await handlerPromise(from, to, resultPromise, j);
    let handlerPromiseCost = Date.now() - handlerPromiseStart;
    console.log('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    logger.info('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    let beforePathDetailOne = promiseResult.pathTypeOne.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    let beforePathDetailTwo = promiseResult.pathTypeTwo.dataDetail.data.pathDetail;
    let beforePathDetailThree = promiseResult.pathTypeThree.dataDetail.data.pathDetail;
    let setSourceTargetStart = Date.now();
    let afterPathDetailOne = setSourceTarget(promiseResult.pathTypeOne.mapRes, beforePathDetailOne);
    let afterPathDetailTwo = setSourceTarget(promiseResult.pathTypeTwo.mapRes, beforePathDetailTwo);
    // let afterPathDetailThree = setSourceTarget(promiseResult.pathTypeThree.mapRes, beforePathDetailThree);
    let setSourceTargetCost = Date.now() - setSourceTargetStart;
    console.log('setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    logger.info('setSourceTargetCost: ' + setSourceTargetCost + 'ms');

    promiseResult.pathTypeOne.dataDetail.data.pathDetail = afterPathDetailOne;                       //用带ITName的pathDetail替换原来的    
    queryNodeResult.nodeResultOne.pathDetail = promiseResult.pathTypeOne.dataDetail;

    promiseResult.pathTypeTwo.dataDetail.data.pathDetail = afterPathDetailTwo;                       //用带ITName的pathDetail替换原来的    
    queryNodeResult.nodeResultTwo.pathDetail = promiseResult.pathTypeTwo.dataDetail;

    promiseResult.pathTypeThree.dataDetail.data.pathDetail = beforePathDetailThree;
    queryNodeResult.nodeResultThree.pathDetail = promiseResult.pathTypeThree.dataDetail;

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

//处理neo4j session9
function sessionRun9(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun9 session close!');
        return result;
    });
}

let searchGraph = {

    //直接投资关系的路径查询
    queryInvestPath: async function (from, to, IVDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let fromIsPerson = 0;
                let toIsPerson = 0;
                if (from.indexOf('P') >= 0) {
                    fromIsPerson = 1;
                }
                if (to.indexOf('P') >= 0) {
                    toIsPerson = 1;
                }
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
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        // let fromOutBoundNum = await cacheHandlers.getCache(`${from}-OutBound`);
                        // if (!fromOutBoundNum) {

                        let fromOutBoundNumStart = Date.now();
                        let retryCountOne = 0;
                        let fromOutBoundNum = 0;
                        do {
                            try {
                                fromOutBoundNum = await getOutBoundNodeNum(from, 2);                                   //获取from的outbound节点数, 查询2层outBound的节点数
                                break;
                            } catch (err) {
                                retryCountOne++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountOne < 3)
                        if (retryCountOne == 3) {
                            console.error('getOutBoundNodeNum execute fail after trying 3 times: ' + from);
                            logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' + from);
                        }
                        let fromOutBoundNumCost = Date.now() - fromOutBoundNumStart;
                        console.log(`${from} fromOutBoundNumCost: ` + fromOutBoundNumCost + 'ms' + ', fromOutBoundNum: ' + fromOutBoundNum);
                        // cacheHandlers.setCache(`${from}-OutBound`, fromOutBoundNum);
                        // }
                        // let toInBoundNum = await cacheHandlers.getCache(`${to}-InBound`);
                        // if (!toInBoundNum) {
                        let toInBoundNumStart = Date.now();
                        let retryCountTwo = 0;
                        let toInBoundNum = 0;
                        do {
                            try {
                                toInBoundNum = await getInBoundNodeNum(to, 2);                                        //获取to的inbound节点数, 查询2层inBound的节点数
                                break;
                            } catch (err) {
                                retryCountTwo++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountTwo < 3)
                        if (retryCountTwo == 3) {
                            console.error('getInBoundNodeNum execute fail after trying 3 times: ' + to);
                            logger.error('getInBoundNodeNum execute fail after trying 3 times: ' + to);
                        }
                        let toInBoundNumCost = Date.now() - toInBoundNumStart;
                        console.log(`${to} toInBoundNumCost: ` + toInBoundNumCost + 'ms' + ', toInBoundNum: ' + toInBoundNum);
                        // cacheHandlers.setCache(`${to}-InBound`, toInBoundNum);
                        // }
                        let investPathQuery = null;
                        let investPathQuery_outBound = null;
                        let investPathQuery_inBound = null;
                        // let investPathQuery_outBound = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo}) match p= (from)-[r:invests*..${IVDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // let investPathQuery_inBound = `start to=node(${nodeIdOne}), from=node(${nodeIdTwo}) match p= (from)<-[r:invests*..${IVDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        // let investPathQuery_outBound = `match p= (from:company{ITCode2: ${from}})-[r:invests*..${IVDepth}]->(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        // let investPathQuery_inBound = `match p= (from:company{ITCode2: ${to}})<-[r:invests*..${IVDepth}]-(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        if (pathType == 'invests' || !pathType) {
                            investPathQuery_outBound = `match p= (from:company{ITCode2: '${from}'})-[r:invests*..${IVDepth}]->(to:company{ITCode2: '${to}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                            investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})<-[r:invests*..${IVDepth}]-(to:company{ITCode2: '${from}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        }
                        else if (pathType == 'all') {
                            //from/to均为自然人时只找family关系
                            if (fromIsPerson == 1 && toIsPerson == 1) {
                                investPathQuery_outBound = `match p= (from:company{ITCode2: '${from}'})-[r:family*]-(to:company{ITCode2: '${to}'}) return p`;
                                investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})-[r:family*]-(to:company{ITCode2: '${from}'}) return p`;
                            }
                            else if (fromIsPerson == 1) {
                                // investPathQuery_outBound = `match p1 = (from:company{ITCode2: '${from}'})-[r1:invests|guarantees|family*..${IVDepth}]->(to:company{ITCode2: '${to}'}), p2 = (:company{ITCode2: '${from}'})-[r2:family*..1]-() return p1,p2`;
                                // investPathQuery_inBound = `match p1 = (from:company{ITCode2: '${to}'})<-[r1:invests|guarantees|family*..${IVDepth}]-(to:company{ITCode2: '${from}'}), p2 = (:company{ITCode2: '${from}'})-[r2:family*..1]-() return p1,p2`;

                                investPathQuery_outBound = `match p1 = (from:company{ITCode2: '${from}'})-[r1:invests|guarantees*..${IVDepth}]->(to:company{ITCode2: '${to}'}) optional match p2 = (from:company{ITCode2: '${from}'})-[r2:family*]-(family), p3 = (family)-[r3:invests|guarantees*..${IVDepth}]->(to:company{ITCode2: '${to}'}) return distinct p1,p2,p3`;
                                investPathQuery_inBound = `match p1 = (from:company{ITCode2: '${to}'})<-[r1:invests|guarantees|family*..${IVDepth}]-(to:company{ITCode2: '${from}'}) optional match p2 = (to:company{ITCode2: '${from}'})-[r3:family*]-(family), p3 = (family)-[r3:invests|guarantees*..${IVDepth}]->(from:company{ITCode2: '${to}'}) return distinct p1,p2,p3`;

                            }
                            // else if (toIsPerson == 1) {
                            //     investPathQuery_outBound = `match p1 = (from:company{ITCode2: '${from}'})-[r1:invests|guarantees|family*..${IVDepth}]->(to:company{ITCode2: '${to}'}), p2 = (:company{ITCode2: '${to}'})-[r2:family*..1]-() return p1,p2`;
                            //     investPathQuery_inBound = `match p1 = (from:company{ITCode2: '${to}'})<-[r1:invests|guarantees|family*..${IVDepth}]-(to:company{ITCode2: '${from}'}), p2 = (:company{ITCode2: '${to}'})-[r2:family*..1]-() return p1,p2`;
                            // }
                            else {
                                investPathQuery_outBound = `match p= (from:company{ITCode2: '${from}'})-[r:invests|guarantees*..${IVDepth}]->(to:company{ITCode2: '${to}'}) return p`;
                                investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})<-[r:invests|guarantees*..${IVDepth}]-(to:company{ITCode2: '${from}'}) return p`;
                            }
                        }

                        //from/to均为自然人时才找family关系
                        // if (fromIsPerson == 1 && toIsPerson == 1) {
                        //     if (pathType == 'invests' || !pathType) {
                        //         investPathQuery_outBound = `match p= (from:company{ITCode2: '${from}'})-[r:invests*..${IVDepth}]->(to:company{ITCode2: '${to}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //         investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})<-[r:invests*..${IVDepth}]-(to:company{ITCode2: '${from}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //     }
                        //     else if (pathType == 'all') {
                        //         investPathQuery_outBound = `match p= (from:company{ITCode2: '${from}'})-[r:invests|guarantees|family*..${IVDepth}]->(to:company{ITCode2: '${to}'}) return p`;
                        //         investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})<-[r:invests|guarantees|family*..${IVDepth}]-(to:company{ITCode2: '${from}'}) return p`;
                        //     }
                        // }
                        // else {
                        //     if (pathType == 'invests' || !pathType) {
                        //         investPathQuery_outBound = `match p= (from:company{ITCode2: '${from}'})-[r:invests*..${IVDepth}]->(to:company{ITCode2: '${to}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //         investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})<-[r:invests*..${IVDepth}]-(to:company{ITCode2: '${from}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //     }
                        //     else if (pathType == 'all') {
                        //         investPathQuery_outBound = `match p1= (from:company{ITCode2: '${from}'})-[r1:invests|guarantees*..${IVDepth}]->(to:company{ITCode2: '${to}'}), p2 = (from:company{ITCode2: '${from}'})-[r2:family]-(), p3 = (to:company{ITCode2: '${to}'})-[r3:family]-() return p1,p2,p3`;
                        //         investPathQuery_inBound = `match p= (from:company{ITCode2: '${to}'})<-[r:invests|guarantees*..${IVDepth}]-(to:company{ITCode2: '${from}'}) return p`;
                        //     }
                        // }

                        if (fromOutBoundNum > toInBoundNum) {
                            investPathQuery = investPathQuery_outBound;
                            console.log('fromOutBoundNum > toInBoundNum: ' + `${fromOutBoundNum} > ${toInBoundNum}`);
                            logger.info('fromOutBoundNum > toInBoundNum: ' + `${fromOutBoundNum} > ${toInBoundNum}`);
                        } else if (fromOutBoundNum <= toInBoundNum) {
                            investPathQuery = investPathQuery_inBound;
                            console.log('fromOutBoundNum <= toInBoundNum: ' + `${fromOutBoundNum} <= ${toInBoundNum}`);
                            logger.info('fromOutBoundNum <= toInBoundNum: ' + `${fromOutBoundNum} <= ${toInBoundNum}`);
                        }

                        //    investPathQuery = `match (from:company{ITCode2:${from}}) -[r1:invests*..2]-> (to),
                        //                             (from:company{ITCode2:${to}}) <-[r2:invests*..2]- (to) 
                        //                             with count(r1) as c1,count(r2) as c2
                        //                       call apoc.case( [(c1 -c2) > 0 , "return (:company{ITCode2: ${from}})-[:invests|guarantees*..5]->(:company{ITCode2: ${to}}) as p",
                        //                                       (c1 -c2) <= 0, "return (:company{ITCode2: ${to}})<-[:invests|guarantees*..5]-(:company{ITCode2: ${from}}) as p"],
                        //                                       'RETURN [] as p') YIELD value
                        //                       return value.p as p`
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            now = Date.now();
                            // resultPromise = await session.run(investPathQuery);

                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun1(investPathQuery);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('sessionRun1 execute fail after trying 3 times: ' + investPathQuery);
                                logger.error('sessionRun1 execute fail after trying 3 times: ' + investPathQuery);
                            }
                            console.log('query neo4j server: ' + investPathQuery);
                            logger.info('query neo4j server: ' + investPathQuery);
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
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                let fromIsPerson = 0;
                let toIsPerson = 0;
                if (from.indexOf('P') >= 0) {
                    fromIsPerson = 1;
                }
                if (to.indexOf('P') >= 0) {
                    toIsPerson = 1;
                }
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
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        // let fromInBoundNum = await cacheHandlers.getCache(`${from}-InBound`);
                        // if (!fromInBoundNum) {
                        let fromInBoundNumStart = Date.now();
                        let retryCountOne = 0;
                        let fromInBoundNum = 0;
                        do {
                            try {
                                fromInBoundNum = await getInBoundNodeNum(from, 2);                                     //获取from的inbound节点数, 查询2层inBound的节点数
                                break;
                            } catch (err) {
                                retryCountOne++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountOne < 3)
                        if (retryCountOne == 3) {
                            console.error('getInBoundNodeNum execute fail after trying 3 times: ' + from);
                            logger.error('getInBoundNodeNum execute fail after trying 3 times: ' + from);
                        }
                        let fromInBoundNumCost = Date.now() - fromInBoundNumStart;
                        console.log(`${from} fromInBoundNumCost: ` + fromInBoundNumCost + 'ms' + ', fromInBoundNum: ' + fromInBoundNum);
                        // cacheHandlers.setCache(`${from}-InBound`, fromInBoundNum);
                        // }
                        // let toOutBoundNum = await cacheHandlers.getCache(`${to}-OutBound`);
                        // if (!toOutBoundNum) {
                        let toOutBoundNumStart = Date.now();
                        let retryCountTwo = 0;
                        let toOutBoundNum = 0;
                        do {
                            try {
                                toOutBoundNum = await getOutBoundNodeNum(to, 2);                                     //获取to的outbound节点数, 查询2层outBound的节点数
                                break;
                            } catch (err) {
                                retryCountTwo++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountTwo < 3)
                        if (retryCountTwo == 3) {
                            console.error('getOutBoundNodeNum execute fail after trying 3 times: ' + to);
                            logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' + to);
                        }
                        let toOutBoundNumCost = Date.now() - toOutBoundNumStart;
                        console.log(`${to} toOutBoundNumCost: ` + toOutBoundNumCost + 'ms' + ', toOutBoundNum: ' + toOutBoundNum);
                        // cacheHandlers.setCache(`${to}-OutBound`, toOutBoundNum);
                        // }
                        let investedByPathQuery = null;
                        let investedByPathQuery_inBound = null;
                        let investedByPathQuery_outBound = null;
                        if (pathType == 'invests' || !pathType) {
                            investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                            investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        }
                        else if (pathType == 'all') {
                            //from/to均为自然人时只找family关系
                            if (fromIsPerson == 1 && toIsPerson == 1) {
                                investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})-[r:family*]-(to:company{ITCode2: '${to}'}) return p`;
                                investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:family*]-(to:company{ITCode2: '${from}'}) return p`;
                            }
                            // else if (fromIsPerson == 1) {
                            //     investedByPathQuery_inBound = `match p1 = (from:company{ITCode2: '${from}'})<-[r1:invests|guarantees|family*..${IVBDepth}]-(to:company{ITCode2: '${to}'}), p2 = (:company{ITCode2: '${from}'})-[r2:family*..1]-() return p1,p2`;
                            //     investedByPathQuery_outBound = `match p1 = (from:company{ITCode2: '${to}'})-[r1:invests|guarantees|family*..${IVBDepth}]->(to:company{ITCode2: '${from}'}), p2 = (:company{ITCode2: '${from}'})-[r2:family*..1]-() return p1,p2`;
                            // }
                            else if (toIsPerson == 1) {
                                // investedByPathQuery_inBound = `match p1 = (from:company{ITCode2: '${from}'})<-[r1:invests|guarantees|family*..${IVBDepth}]-(to:company{ITCode2: '${to}'}), p2 = (:company{ITCode2: '${to}'})-[r2:family*..1]-() return p1,p2`;
                                // investedByPathQuery_outBound = `match p1 = (from:company{ITCode2: '${to}'})-[r1:invests|guarantees|family*..${IVBDepth}]->(to:company{ITCode2: '${from}'}), p2 = (:company{ITCode2: '${to}'})-[r2:family*..1]-() return p1,p2`;

                                investedByPathQuery_inBound = `match p1 = (from:company{ITCode2: '${from}'})<-[r1:invests|guarantees*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) optional match p2 = (to:company{ITCode2: '${to}'})-[r2:family*]-(family), p3 = (from:company{ITCode2: '${from}'})<-[r3:invests|guarantees*..${IVBDepth}]-(family) return distinct p1,p2,p3`;
                                investedByPathQuery_outBound = `match p1 = (from:company{ITCode2: '${to}'})-[r1:invests|guarantees*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) optional match p2 = (from:company{ITCode2: '${to}'})-[r2:family*]-(family), p3 = (family)-[r3:invests|guarantees*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) return distinct p1,p2,p3`;
                            }
                            else {
                                investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})<-[r:invests|guarantees*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) return p`;
                                investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:invests|guarantees*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) return p`;
                            }
                        }
                        // let investedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: ${to}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        // let investedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: ${from}}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;    
                        //from/to均为自然人时才找family关系
                        // if (fromIsPerson == 1 && toIsPerson == 1) {
                        //     if (pathType == 'invests' || !pathType) {
                        //         investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //         investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //     }
                        //     else if (pathType == 'all') {
                        //         investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})<-[r:invests|guarantees|family*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) return p`;
                        //         investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:invests|guarantees|family*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) return p`;
                        //     }
                        // }
                        // else {
                        //     if (pathType == 'invests' || !pathType) {
                        //         investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})<-[r:invests*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //         investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:invests*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        //     }
                        //     else if (pathType == 'all') {
                        //         investedByPathQuery_inBound = `match p= (from:company{ITCode2: '${from}'})<-[r:invests|guarantees*..${IVBDepth}]-(to:company{ITCode2: '${to}'}) return p`;
                        //         investedByPathQuery_outBound = `match p= (from:company{ITCode2: '${to}'})-[r:invests|guarantees*..${IVBDepth}]->(to:company{ITCode2: '${from}'}) return p`;
                        //     }
                        // }

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
                            console.log('toOutBoundNum < fromInBoundNum: ' + `${toOutBoundNum} < ${fromInBoundNum}`);
                            logger.log('toOutBoundNum < fromInBoundNum: ' + `${toOutBoundNum} < ${fromInBoundNum}`);
                        } else if (toOutBoundNum >= fromInBoundNum) {
                            investedByPathQuery = investedByPathQuery_outBound;
                            console.log('toOutBoundNum >= fromInBoundNum: ' + `${toOutBoundNum} >= ${fromInBoundNum}`);
                            logger.info('toOutBoundNum >= fromInBoundNum: ' + `${toOutBoundNum} >= ${fromInBoundNum}`);
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            now = Date.now();
                            // resultPromise = await session.run(investedByPathQuery);

                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun2(investedByPathQuery);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('sessionRun2 execute fail after trying 3 times: ' + investedByPathQuery);
                                logger.error('sessionRun2 execute fail after trying 3 times: ' + investedByPathQuery);
                            }
                            console.log('query neo4j server: ' + investedByPathQuery);
                            logger.info('query neo4j server: ' + investedByPathQuery);
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
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                let fromIsPerson = 0;
                let toIsPerson = 0;
                if (from.indexOf('P') >= 0) {
                    fromIsPerson = 1;
                }
                if (to.indexOf('P') >= 0) {
                    toIsPerson = 1;
                }
                let j = 2;                                                             //记录每种路径查询方式索引号,ShortestPathQuery索引为2
                let cacheKey = [j, from, to, lowWeight, highWeight, pathType].join('-');
                // let nodeIdOne = await findNodeId(from);
                // let nodeIdTwo = await findNodeId(to);
                // let shortestPathQuery = `start from=node(${nodeIdOne}), to=node(${nodeIdTwo})   match p= allShortestPaths((from)-[r:invests*]-(to)) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                // let shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: ${from}})-[r:invests*]-(to:company{ITCode2: ${to}})) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and from.isExtra = '0' and to.isExtra = '0') return p`;
                let shortestPathQuery = null;
                //from/to均为自然人时才找family关系
                if (fromIsPerson == 1 && toIsPerson == 1) {
                    if (pathType == 'invests' || !pathType) {
                        shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: '${from}'})-[r:invests*]-(to:company{ITCode2: '${to}'})) using index from:company(ITCode2) using index to:company(ITCode2) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p limit 10`;
                    }
                    else if (pathType == 'all') {
                        shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: '${from}'})-[r:invests|guarantees|family*]-(to:company{ITCode2: '${to}'})) using index from:company(ITCode2) using index to:company(ITCode2) return p limit 10`;
                    }
                }
                else {
                    if (pathType == 'invests' || !pathType) {
                        shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: '${from}'})-[r:invests*]-(to:company{ITCode2: '${to}'})) using index from:company(ITCode2) using index to:company(ITCode2) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p limit 10`;
                    }
                    else if (pathType == 'all') {
                        shortestPathQuery = `match p= allShortestPaths((from:company{ITCode2: '${from}'})-[r:invests|guarantees*]-(to:company{ITCode2: '${to}'})) using index from:company(ITCode2) using index to:company(ITCode2) return p limit 10`;
                    }
                }

                if (from == null || to == null) {
                    console.error(`${from} or ${to} is not in the neo4j database at all !`);
                    logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                    // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                    let nodeResults = {
                        nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'ShortestPath', names: [], codes: [] } },
                        nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                        nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                            console.error('sessionRun3 execute fail after trying 3 times: ' + shortestPathQuery);
                            logger.error('sessionRun3 execute fail after trying 3 times: ' + shortestPathQuery);
                        }
                        console.log('query neo4j server: ' + shortestPathQuery);
                        logger.info('query neo4j server: ' + shortestPathQuery);
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
                                nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                let fromIsPerson = 0;
                let toIsPerson = 0;
                if (from.indexOf('P') >= 0) {
                    fromIsPerson = 1;
                }
                if (to.indexOf('P') >= 0) {
                    toIsPerson = 1;
                }
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
                    //from/to均为自然人时才找family关系
                    if (fromIsPerson == 1 && toIsPerson == 1) {
                        if (pathType == 'invests' || !pathType) {
                            fullPathQuery = `match p= (from:company{ITCode2: '${from}'})-[r:invests*..${FUDepth}]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        }
                        else if (pathType == 'all') {
                            fullPathQuery = `match p= (from:company{ITCode2: '${from}'})-[r:invests|guarantees|family*..${FUDepth}]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                        }
                    }
                    else {
                        if (pathType == 'invests' || !pathType) {
                            fullPathQuery = `match p= (from:company{ITCode2: '${from}'})-[r:invests*..${FUDepth}]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight}) return p`;
                        }
                        else if (pathType == 'all') {
                            fullPathQuery = `match p= (from:company{ITCode2: '${from}'})-[r:invests|guarantees*..${FUDepth}]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                        }
                    }

                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'FullPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                                console.error('sessionRun4 execute fail after trying 3 times: ' + fullPathQuery);
                                logger.error('sessionRun4 execute fail after trying 3 times: ' + fullPathQuery);
                            }
                            console.log('query neo4j server: ' + fullPathQuery);
                            logger.info('query neo4j server: ' + fullPathQuery);
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
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        // let fromOutBoundNum = await cacheHandlers.getCache(`${from}-OutBound`);
                        // if (!fromOutBoundNum) {
                        let fromOutBoundNumStart = Date.now();
                        let retryCountOne = 0;
                        let fromOutBoundNum = 0;
                        do {
                            try {
                                fromOutBoundNum = await getOutBoundNodeNum(from, 1);                                   //获取from的outbound节点数, 查询1层outBound的节点数
                                break;
                            } catch (err) {
                                retryCountOne++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountOne < 3)
                        if (retryCountOne == 3) {
                            console.error('getOutBoundNodeNum execute fail after trying 3 times: ' + from);
                            logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' + from);
                        }
                        let fromOutBoundNumCost = Date.now() - fromOutBoundNumStart;
                        console.log(`${from} fromOutBoundNumCost: ` + fromOutBoundNumCost + 'ms' + ', fromOutBoundNum: ' + fromOutBoundNum);
                        // cacheHandlers.setCache(`${from}-OutBound`, fromOutBoundNum);
                        // }
                        // let toOutBoundNum = await cacheHandlers.getCache(`${to}-OutBound`);
                        // if (!toOutBoundNum) {
                        let toOutBoundNumStart = Date.now();
                        let retryCountTwo = 0;
                        let toOutBoundNum = 0;
                        do {
                            try {
                                toOutBoundNum = await getOutBoundNodeNum(to, 1);                                     //获取to的inbound节点数, 查询1层outBound的节点数
                                break;
                            } catch (err) {
                                retryCountTwo++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountTwo < 3)
                        if (retryCountTwo == 3) {
                            console.error('getOutBoundNodeNum execute fail after trying 3 times: ' + to);
                            logger.error('getOutBoundNodeNum execute fail after trying 3 times: ' + to);
                        }
                        let toOutBoundNumCost = Date.now() - toOutBoundNumStart;
                        console.log(`${to} toOutBoundNumCost: ` + toOutBoundNumCost + 'ms' + ', toOutBoundNum: ' + toOutBoundNum);
                        // cacheHandlers.setCache(`${to}-OutBound`, toOutBoundNum);
                        // }
                        let commonInvestPathQuery = null;
                        // let commonInvestPathQueryOne = `match p= (from:company{ITCode2: ${from}})-[r1:invests*..${CIVDepth/2}]->()<-[r2:invests*..${CIVDepth/2}]-(to:company{ITCode2: ${to}}) where from.isExtra = '0' and to.isExtra = '0' return p`;                    
                        // let commonInvestPathQueryTwo = `match p= (from:company{ITCode2: ${to}})-[r1:invests*..${CIVDepth/2}]->()<-[r2:invests*..${CIVDepth/2}]-(to:company{ITCode2: ${from}}) where from.isExtra = '0' and to.isExtra = '0' return p`;                    
                        let commonInvestPathQueryOne = null;
                        let commonInvestPathQueryTwo = null;
                        if (pathType == 'invests' || !pathType) {
                            commonInvestPathQueryOne = `match p= (from:company{ITCode2: '${from}'})-[r1:invests*..${CIVDepth / 2}]->()<-[r2:invests*..${CIVDepth / 2}]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                            commonInvestPathQueryTwo = `match p= (from:company{ITCode2: '${to}'})-[r1:invests*..${CIVDepth / 2}]->()<-[r2:invests*..${CIVDepth / 2}]-(to:company{ITCode2: '${from}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                        }
                        else if (pathType == 'all') {
                            //判断from、to是否自然人的personalCode
                            let fromIsPerson = 0;
                            let toIsPerson = 0;
                            if (from.indexOf('P') >= 0) {
                                fromIsPerson = 1;
                            }
                            if (to.indexOf('P') >= 0) {
                                toIsPerson = 1;
                            }
                            //当from/to均为自然人时加入p2, 查询from/to是否含有family关系, p1不需要查询family关系
                            if (fromIsPerson == 1 && toIsPerson == 1) {
                                commonInvestPathQueryOne = `match p1 = (from:company{ITCode2: '${from}'})-[r1:invests|guarantees*..${CIVDepth / 2}]->()<-[r2:invests|guarantees*..${CIVDepth / 2}]-(to:company{ITCode2: '${to}'}), p2 = (from:company{ITCode2: '${from}'})-[r3:family*..1]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p1,p2`;
                                commonInvestPathQueryTwo = `match p1 = (from:company{ITCode2: '${to}'})-[r1:invests|guarantees*..${CIVDepth / 2}]->()<-[r2:invests|guarantees*..${CIVDepth / 2}]-(to:company{ITCode2: '${from}'}), p2 = (from:company{ITCode2: '${from}'})-[r3:family*..1]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p1,p2`;
                            }
                            else {
                                commonInvestPathQueryOne = `match p= (from:company{ITCode2: '${from}'})-[r1:invests|guarantees|family*..${CIVDepth / 2}]->()<-[r2:invests|guarantees|family*..${CIVDepth / 2}]-(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                                commonInvestPathQueryTwo = `match p= (from:company{ITCode2: '${to}'})-[r1:invests|guarantees|family*..${CIVDepth / 2}]->()<-[r2:invests|guarantees|family*..${CIVDepth / 2}]-(to:company{ITCode2: '${from}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                            }
                        }
                        if (fromOutBoundNum > toOutBoundNum) {
                            commonInvestPathQuery = commonInvestPathQueryOne;
                            console.log('fromOutBoundNum > toOutBoundNum: ' + `${fromOutBoundNum} > ${toOutBoundNum}`);
                            logger.info('fromOutBoundNum > toOutBoundNum: ' + `${fromOutBoundNum} > ${toOutBoundNum}`);
                        } else if (fromOutBoundNum <= toOutBoundNum) {
                            commonInvestPathQuery = commonInvestPathQueryTwo;
                            console.log('fromOutBoundNum <= toOutBoundNum: ' + `${fromOutBoundNum} <= ${toOutBoundNum}`);
                            logger.info('fromOutBoundNum <= toOutBoundNum: ' + `${fromOutBoundNum} <= ${toOutBoundNum}`);
                        }
                        let now = 0;
                        //缓存
                        // let key = [j, commonInvestPathQuery].join('-');
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let commonInvestPathQueryCost = 0;
                            now = Date.now();
                            // resultPromise = await session.run(commonInvestPathQuery);

                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun5(commonInvestPathQuery);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('sessionRun5 execute fail after trying 3 times: ' + commonInvestPathQuery);
                                logger.error('sessionRun5 execute fail after trying 3 times: ' + commonInvestPathQuery);
                            }
                            console.log('query neo4j server: ' + commonInvestPathQuery);
                            logger.info('query neo4j server: ' + commonInvestPathQuery);
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
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        // let fromInBoundNum = await cacheHandlers.getCache(`${from}-InBound`);
                        // if (!fromInBoundNum) {
                        let fromInBoundNumStart = Date.now();
                        let retryCountOne = 0;
                        let fromInBoundNum = 0;
                        do {
                            try {
                                fromInBoundNum = await getInBoundNodeNum(from, 1);                                     //获取from的inbound节点数, 查询1层inBound的节点数
                                break;
                            } catch (err) {
                                retryCountOne++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountOne < 3)
                        if (retryCountOne == 3) {
                            console.error('getInBoundNodeNum execute fail after trying 3 times: ' + from);
                            logger.error('getInBoundNodeNum execute fail after trying 3 times: ' + from);
                        }
                        let fromInBoundNumCost = Date.now() - fromInBoundNumStart;
                        console.log(`${from} fromInBoundNumCost: ` + fromInBoundNumCost + 'ms' + ', fromInBoundNum: ' + fromInBoundNum);
                        // cacheHandlers.setCache(`${from}-InBound`, fromInBoundNum);
                        // }
                        // let toInBoundNum = await cacheHandlers.getCache(`${to}-InBound`);
                        // if (!toInBoundNum) {
                        let toInBoundNumStart = Date.now();
                        let retryCountTwo = 0;
                        do {
                            try {
                                toInBoundNum = await getInBoundNodeNum(to, 1);                                       //获取to的outbound节点数, 查询1层inBound的节点数
                                break;
                            } catch (err) {
                                retryCountTwo++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountTwo < 3)
                        if (retryCountTwo == 3) {
                            console.error('getInBoundNodeNum execute fail after trying 3 times: ' + to);
                            logger.error('getInBoundNodeNum execute fail after trying 3 times: ' + to);
                        }
                        let toInBoundNumCost = Date.now() - toInBoundNumStart;
                        console.log(`${to} toInBoundNumCost: ` + toInBoundNumCost + 'ms' + ', toInBoundNum: ' + toInBoundNum);
                        // cacheHandlers.setCache(`${to}-InBound`, toInBoundNum);
                        // }
                        let commonInvestedByPathQuery = null;
                        let commonInvestedByPathQueryOne = null;
                        let commonInvestedByPathQueryTwo = null;
                        if (pathType == 'invests' || !pathType) {
                            if (isExtra == 0 || isExtra == '0') {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: '${from}'})<-[r1:invests*..${CIVBDepth / 2}]-(com)-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: '${to}'})<-[r1:invests*..${CIVBDepth / 2}]-(com)-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: '${from}'}) using index from:company(ITCode2) using index to:company(ITCode2) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                            }
                            else {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: '${from}'})<-[r1:invests*..${CIVBDepth / 2}]-()-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: '${to}'})<-[r1:invests*..${CIVBDepth / 2}]-()-[r2:invests*..${CIVBDepth / 2}]->(to:company{ITCode2: '${from}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                            }
                        }
                        else if (pathType == 'all') {
                            if (isExtra == 0 || isExtra == '0') {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: '${from}'})<-[r1:invests|guarantees|family*..${CIVBDepth / 2}]-(com)-[r2:invests|guarantees|family*..${CIVBDepth / 2}]->(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: '${to}'})<-[r1:invests|guarantees|family*..${CIVBDepth / 2}]-(com)-[r2:invests|guarantees|family*..${CIVBDepth / 2}]->(to:company{ITCode2: '${from}'}) using index from:company(ITCode2) using index to:company(ITCode2) where from.isExtra = '0' and to.isExtra = '0' and com.isExtra = '0' return p`;
                            }
                            else {
                                commonInvestedByPathQueryOne = `match p= (from:company{ITCode2: '${from}'})<-[r1:invests|guarantees|family*..${CIVBDepth / 2}]-()-[r2:invests|guarantees|family*..${CIVBDepth / 2}]->(to:company{ITCode2: '${to}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                                commonInvestedByPathQueryTwo = `match p= (from:company{ITCode2: '${to}'})<-[r1:invests|guarantees|family*..${CIVBDepth / 2}]-()-[r2:invests|guarantees|family*..${CIVBDepth / 2}]->(to:company{ITCode2: '${from}'}) using index from:company(ITCode2) using index to:company(ITCode2) return p`;
                            }
                        }

                        if (toInBoundNum < fromInBoundNum) {
                            commonInvestedByPathQuery = commonInvestedByPathQueryOne;
                            console.log('toInBoundNum < fromInBoundNum: ' + `${toInBoundNum} < ${fromInBoundNum}`);
                            logger.info('toInBoundNum < fromInBoundNum: ' + `${toInBoundNum} < ${fromInBoundNum}`);
                        } else if (toInBoundNum >= fromInBoundNum) {
                            commonInvestedByPathQuery = commonInvestedByPathQueryTwo;
                            console.log('toInBoundNum >= fromInBoundNum: ' + `${toInBoundNum} >= ${fromInBoundNum}`);
                            logger.info('toInBoundNum >= fromInBoundNum: ' + `${toInBoundNum} >= ${fromInBoundNum}`);
                        }
                        let now = 0;
                        //缓存
                        // let key = [j, commonInvestedByPathQuery].join('-');
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let commonInvestedByPathQueryCost = 0;
                            now = Date.now();
                            // resultPromise = await session.run(commonInvestedByPathQuery);

                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await sessionRun6(commonInvestedByPathQuery);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('sessionRun6 execute fail after trying 3 times: ' + commonInvestedByPathQuery);
                                logger.error('sessionRun6 execute fail after trying 3 times: ' + commonInvestedByPathQuery);
                            }
                            console.log('query neo4j server: ' + commonInvestedByPathQuery);
                            logger.info('query neo4j server: ' + commonInvestedByPathQuery);
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
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
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

    //高管投资关系路径查询
    queryExecutiveInvestPath: async function (code, surStatus) {
        return new Promise(async function (resolve, reject) {

            try {
                let now = Date.now();
                let j = 12;                                                              //记录每种路径查询方式索引号,directInvestPathQuery索引为6
                let pathType = 'execute';
                let cacheKey = [j, code, pathType].join('-');
                //缓存
                let previousValue = await cacheHandlers.getCache(cacheKey);
                if (!previousValue) {
                    let executiveInvestPathQuery = null;
                    if (surStatus == 0) {
                        executiveInvestPathQuery = `match p= (from:company{ITCode2: '${code}'})-[r:executes]->(to) return p`;
                    }
                    else if (surStatus == 1 || surStatus == '1') {
                        executiveInvestPathQuery = `match p= (from:company{ITCode2: '${code}'})-[r:executes]->(to) where to.surStatus = '1' return p`;
                    }
                    let retryCount = 0;
                    let resultPromise = null;
                    do {
                        try {
                            resultPromise = await sessionRun9(executiveInvestPathQuery);
                            break;
                        } catch (err) {
                            retryCount++;
                            console.error(err);
                            logger.error(err);
                        }
                    } while (retryCount < 3)
                    if (retryCount == 3) {
                        console.error('sessionRun9 execute fail after trying 3 times: ' + executiveInvestPathQuery);
                        logger.error('sessionRun9 execute fail after trying 3 times: ' + executiveInvestPathQuery);
                    }
                    console.log('query neo4j server: ' + executiveInvestPathQuery);
                    logger.info('query neo4j server: ' + executiveInvestPathQuery);
                    let executiveInvestPathQueryCost = Date.now() - now;
                    logger.info(`code: ${code}` + " executiveInvestPathQueryCost: " + executiveInvestPathQueryCost + 'ms');
                    console.log("time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` code: ${code} ` + ",  executiveInvestPathQueryCost: " + executiveInvestPathQueryCost + 'ms');
                    if (resultPromise.records.length > 0) {
                        let result = await handlerPromise(null, null, resultPromise, j);
                        let afterPathDetailFour = setSourceTarget2(result.pathTypeFour.mapRes, result.pathTypeFour.dataDetail.data.pathDetail);
                        let sortPathDetailFour = afterPathDetailFour.sort(sortRegName);                                   //按注册资本、名称排序
                        result.pathTypeFour.dataDetail.data.pathDetail = sortPathDetailFour;
                        let newResult = result.pathTypeFour.dataDetail;
                        // cacheHandlers.setCache(cacheKey, JSON.stringify(newResult));
                        return resolve(newResult);
                    }
                    else {
                        return resolve({ data: { pathDetail: [], pathNum: 0 }, type: `result_12`, typeName: 'executiveInvestPath', names: [] });
                    }
                }
                else if (previousValue) {
                    return resolve(JSON.parse(previousValue));
                }
            }
            catch (err) {
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
                        // let fromOutBoundNum = await cacheHandlers.getCache(`${from}-OutBound`);
                        // if (!fromOutBoundNum) {
                        let fromOutBoundNumStart = Date.now();
                        let retryCountOne = 0;
                        let fromOutBoundNum = 0;
                        do {
                            try {
                                fromOutBoundNum = await getGuaranteeOutBoundNodeNum(from, 1);                                   //获取from的outbound节点数
                                break;
                            } catch (err) {
                                retryCountOne++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountOne < 3)
                        if (retryCountOne == 3) {
                            console.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' + from);
                            logger.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' + from);
                        }
                        let fromOutBoundNumCost = Date.now() - fromOutBoundNumStart;
                        console.log(`${from} fromOutBoundNumCost: ` + fromOutBoundNumCost + 'ms' + ', fromOutBoundNum: ' + fromOutBoundNum);
                        // cacheHandlers.setCache(`${from}-OutBound`, fromOutBoundNum);
                        // }
                        // let toInBoundNum = await cacheHandlers.getCache(`${to}-InBound`);
                        // if (!toInBoundNum) {
                        let toInBoundNumStart = Date.now();
                        let retryCountTwo = 0;
                        let toInBoundNum = 0;
                        do {
                            try {
                                toInBoundNum = await getGuaranteeInBoundNodeNum(to, 1);                                       //获取to的inbound节点数
                                break;
                            } catch (err) {
                                retryCountTwo++
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountTwo < 3)
                        if (retryCountTwo == 3) {
                            console.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' + to);
                            logger.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' + to);
                        }
                        let toInBoundNumCost = Date.now() - toInBoundNumStart;
                        console.log(`${to} toInBoundNumCost: ` + toInBoundNumCost + 'ms' + ', toInBoundNum: ' + toInBoundNum);
                        // cacheHandlers.setCache(`${to}-InBound`, toInBoundNum);
                        // }
                        let guaranteePathQuery = null;
                        let guaranteePathQuery_outBound = `match p= (from:company{ITCode2: ${from}})-[r:guarantees*..${GTDepth}]->(to:company{ITCode2: ${to}}) return p`;
                        let guaranteePathQuery_inBound = `match p= (from:company{ITCode2: ${to}})<-[r:guarantees*..${GTDepth}]-(to:company{ITCode2: ${from}}) return p`;
                        if (fromOutBoundNum > toInBoundNum) {
                            guaranteePathQuery = guaranteePathQuery_outBound;
                            console.log('fromOutBoundNum > toInBoundNum: ' + `${fromOutBoundNum} > ${toInBoundNum}`);
                            logger.info('fromOutBoundNum > toInBoundNum: ' + `${fromOutBoundNum} > ${toInBoundNum}`);
                        } else if (fromOutBoundNum <= toInBoundNum) {
                            guaranteePathQuery = guaranteePathQuery_inBound;
                            console.log('fromOutBoundNum <= toInBoundNum: ' + `${fromOutBoundNum} <= ${toInBoundNum}`);
                            logger.info('fromOutBoundNum <= toInBoundNum: ' + `${fromOutBoundNum} <= ${toInBoundNum}`);
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let resultPromise = null;
                            now = Date.now();
                            // resultPromise = await session.run(guaranteePathQuery);

                            let retryCountThree = 0;
                            do {
                                try {
                                    resultPromise = await sessionRun7(guaranteePathQuery);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('sessionRun7 execute fail after trying 3 times: ' + guaranteePathQuery);
                                logger.error('sessionRun7 execute fail after trying 3 times: ' + guaranteePathQuery);
                            }
                            console.log('query neo4j server: ' + guaranteePathQuery);
                            logger.info('query neo4j server: ' + guaranteePathQuery);
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
                        // let fromInBoundNum = await cacheHandlers.getCache(`${from}-InBound`);
                        // if (!fromInBoundNum) {
                        let fromInBoundNumStart = Date.now();
                        let retryCountOne = 0;
                        let fromInBoundNum = 0;
                        do {
                            try {
                                fromInBoundNum = await getGuaranteeInBoundNodeNum(from, 1);                                     //获取from的inbound节点数
                                break;
                            } catch (err) {
                                retryCountOne++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountOne < 3)
                        if (retryCountOne == 3) {
                            console.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' + from);
                            logger.error('getGuaranteeInBoundNodeNum execute fail after trying 3 times: ' + from);
                        }
                        let fromInBoundNumCost = Date.now() - fromInBoundNumStart;
                        console.log(`${from} fromInBoundNumCost: ` + fromInBoundNumCost + 'ms' + ', fromInBoundNum: ' + fromInBoundNum);
                        // cacheHandlers.setCache(`${from}-InBound`, fromInBoundNum);
                        // }
                        // let toOutBoundNum = await cacheHandlers.getCache(`${to}-OutBound`);
                        // if (!toOutBoundNum) {
                        let toOutBoundNumStart = Date.now();
                        let retryCountTwo = 0;
                        let toOutBoundNum = 0;
                        do {
                            try {
                                toOutBoundNum = await getGuaranteeOutBoundNodeNum(to, 1);                                     //获取to的outbound节点数
                                break;
                            } catch (err) {
                                retryCountTwo++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCountTwo < 3)
                        if (retryCountTwo == 3) {
                            console.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' + to);
                            logger.error('getGuaranteeOutBoundNodeNum execute fail after trying 3 times: ' + to);
                        }
                        let toOutBoundNumCost = Date.now() - toOutBoundNumStart;
                        console.log(`${to} toOutBoundNumCost: ` + toOutBoundNumCost + 'ms' + ', toOutBoundNum: ' + toOutBoundNum);
                        // cacheHandlers.setCache(`${to}-OutBound`, toOutBoundNum);
                        // }
                        let guaranteedByPathQuery = null;
                        let guaranteedByPathQuery_inBound = `match p= (from:company{ITCode2: ${from}})<-[r:guarantees*..${GTBDepth}]-(to:company{ITCode2: ${to}}) return p`;
                        let guaranteedByPathQuery_outBound = `match p= (from:company{ITCode2: ${to}})-[r:guarantees*..${GTBDepth}]->(to:company{ITCode2: ${from}}) return p`;
                        if (toOutBoundNum < fromInBoundNum) {
                            guaranteedByPathQuery = guaranteedByPathQuery_inBound;
                            console.log('toOutBoundNum < fromInBoundNum: ' + `${toOutBoundNum} < ${fromInBoundNum}`);
                            logger.info('toOutBoundNum < fromInBoundNum: ' + `${toOutBoundNum} < ${fromInBoundNum}`);
                        } else if (toOutBoundNum >= fromInBoundNum) {
                            guaranteedByPathQuery = guaranteedByPathQuery_outBound;
                            console.log('toOutBoundNum >= fromInBoundNum: ' + `${toOutBoundNum} >= ${fromInBoundNum}`);
                            logger.info('toOutBoundNum >= fromInBoundNum: ' + `${toOutBoundNum} >= ${fromInBoundNum}`);
                        }
                        let now = 0;
                        //缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        if (!previousValue) {
                            let resultPromise = null;
                            now = Date.now();
                            // resultPromise = await session.run(guaranteedByPathQuery);

                            let retryCountThree = 0;
                            do {
                                try {
                                    resultPromise = await sessionRun8(guaranteedByPathQuery);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('sessionRun8 execute fail after trying 3 times: ' + guaranteedByPathQuery);
                                logger.error('sessionRun8 execute fail after trying 3 times: ' + guaranteedByPathQuery);
                            }
                            console.log('query neo4j server: ' + guaranteedByPathQuery);
                            logger.info('query neo4j server: ' + guaranteedByPathQuery);
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