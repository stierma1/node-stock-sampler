var install = require("installation-station");
var PouchDB = require("pouchdb");

(async function() {
    await install("stock-sampler", 1, [{
        question: "Where is the daily config located?: ",
        key: "dailyConfigLocation"
    }, {
        question: "Should I upload the stock symbols to the daily config location?(y/n): ",
        key: "uploadSymbolsDaily"
    }, {
        question: "Should I upload the expected fields to the daily config location?(y/n): ",
        key: "uploadFieldsDaily"
    }, {
        question: "Please give polling schedule for daily: ",
        key: "dailySchedule"
    }, {
        question: "Where is the realtime config located?: ",
        key: "realtimeConfigLocation"
    }, {
        question: "What will the realtime concurrency be?: ",
        key: "realtimeConcurrency"
    }, {
        question: "Should I upload the stock symbols to the realtime config location?(y/n): ",
        key: "uploadSymbolsRealtime"
    }, {
        question: "Should I upload the expected fields to the realtime config location?(y/n): ",
        key: "uploadFieldsRealtime"
    }, {
        question: "Please give polling schedule for realtime: ",
        key: "realtimeSchedule"
    }], "local")

    var nEw = require("pid-async-class").nEw;
    var Configurator = require("./configurator");

    var configurator = await nEw(Configurator);

    var config = await configurator.getConfiguration();
    var dailyStorage = new PouchDB(config.dailyConfigLocation);
    if (config.uploadSymbolsDaily.toLowerCase() === "y") {
        await dailyStorage.put({
            _id: "symbols",
            symbols: require("./default-config.json").symbols
        });
    }

    if (config.uploadFieldsDaily.toLowerCase() === "y") {
        await dailyStorage.put({
            _id: "fields",
            fields: require("./default-config.json").dailyFields
        });
    }

    var realtimeStorage = new PouchDB(config.realtimeConfigLocation);
    if (config.uploadSymbolsRealtime.toLowerCase() === "y") {
        await realtimeStorage.put({
            _id: "symbols",
            symbols: require("./default-config.json").symbols
        });
    }

    if (config.uploadFieldsRealtime.toLowerCase() === "y") {
        await realtimeStorage.put({
            _id: "fields",
            fields: require("./default-config.json").realtimeFields
        });
    }
})()
