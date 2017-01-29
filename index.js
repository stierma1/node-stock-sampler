var Configurator = require("./configurator");
var Storage = require("./lib/storage");
var Stocks = require("./lib/stocks");
var nEw = require("pid-async-class").nEw;

(async function(){
  var configurator = await nEw(Configurator);

  await configurator.register("configuration");
  var sample = await nEw(Storage, (await configurator.getConfiguration()).realtimeConfigLocation);
  sample.register("realtime-storage");
  var storage = await nEw(Storage, (await configurator.getConfiguration()).dailyConfigLocation);
  storage.register("daily-storage");
  var stocks = await nEw(Stocks);
})()
