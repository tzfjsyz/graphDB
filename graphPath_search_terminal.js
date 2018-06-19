const Hapi = require('hapi');
const server = new Hapi.Server();
const apiHandlers = require("./graphSearch/apiHandlers.js");
// require('events').EventEmitter.prototype._maxListeners = 100;

server.connection({
    port: 8290,
    routes: {
        cors: true
    },
    compression: true
});

//企业关系所有路径(6种)查询
// server.route({
//     method: 'GET',
//     path: '/queryAllPath',
//     handler: apiHandlers.queryInfo
// });

//直接投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryInvestPath',
    handler: apiHandlers.queryInvestPathInfo
});

//直接被投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryInvestedByPath',
    handler: apiHandlers.queryInvestedByPathInfo
});

//最短路径查询
server.route({
    method: 'GET',
    path: '/queryShortestPath',
    handler: apiHandlers.queryShortestPathInfo
});

//全部路径查询
server.route({
    method: 'GET',
    path: '/queryFullPath',
    handler: apiHandlers.queryFullPathInfo
});

//共同投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryCommonInvestPath',
    handler: apiHandlers.queryCommonInvestPathInfo
});

//共同被投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryCommonInvestedByPath',
    handler: apiHandlers.queryCommonInvestedByPathInfo
});

//单个企业直接投资关系路径查询
// server.route({
//     method: 'GET',
//     path: '/queryDirectInvestPath',
//     handler: apiHandlers.queryDirectInvestPathInfo
// });

// //单个企业直接被投资关系路径查询
// server.route({
//     method: 'GET',
//     path: '/queryDirectInvestedByPath',
//     handler: apiHandlers.queryDirectInvestedByPathInfo
// });

//担保关系路径查询
server.route({
    method: 'GET',
    path: '/queryGuaranteePath',
    handler: apiHandlers.queryGuaranteePathInfo
});

//被担保关系路径查询
server.route({
    method: 'GET',
    path: '/queryGuaranteedByPath',
    handler: apiHandlers.queryGuaranteedByPathInfo
});

//手动添加需要预加热的from/to/depth/relation
server.route({
    method: 'GET',
    path: '/addWarmUpQueryData',
    handler: apiHandlers.addWarmUpQueryDataInfo
});

//手动删除需要预加热的from/to/depth/relation
server.route({
    method: 'GET',
    path: '/deleteWarmUpQueryData',
    handler: apiHandlers.deleteWarmUpQueryDataInfo
});

//查询所有的预热数据的key对应的field
server.route({
    method: 'GET',
    path: '/listWarmUpConditionsField',
    handler: apiHandlers.listWarmUpConditionsFieldInfo
});

//外部接口触发path数据预热
server.route({
    method: 'GET',
    path: '/warmUpPaths',
    handler: apiHandlers.startWarmUpPaths
});

//外部调用接口删除lockResource
server.route({
    method: 'GET',
    path: '/deleteLockResource',
    handler: apiHandlers.deleteLockResource
});

server.start((err) => {
    if (err) {
        throw err;
    }
    console.info(`企业关系路径搜索API服务运行在:${server.info.uri}`);
});

process.on('unhandledRejection', (err) => {
    console.log(err);
    console.log('NOT exit...');
    process.exit(1);
});