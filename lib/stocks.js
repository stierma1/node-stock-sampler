
var ScheduledAsync = require("pid-async-class").ScheduledAsync;
var nEw = require("pid-async-class").nEw;
var BaseAsync = require("pid-async-class").BaseAsync;
var yahooFinance = require('yahoo-finance');
var System = require("pid-system")

class Stocks extends ScheduledAsync{
  constructor(){
    super(true);
    this.symbols = [];
    this.concurrency = 1;
    this.scheduled = 900000;
    this.fields = [];

    Promise.resolve(this._init())
    .then(() => {
      this.__completeConstruction();
    })
    .catch((err) => {
      console.log(err)
    })

  }

  async _init(){
    var configurator = await this.resolve("configuration");
    var dailyStorage = await this.resolve("daily-storage");
    var realtimeStorage = await this.resolve("realtime-storage");
    this.symbols = (await realtimeStorage.get("symbols")).symbols;

    this.concurrency =  parseInt((await configurator.getConfiguration()).realtimeConcurrency) || 1;
    this.dailySchedule = (await configurator.getConfiguration()).dailySchedule || "* 24 * * * *";
    this.sampleSchedule = (await configurator.getConfiguration()).realtimeSchedule || 900000;
    this.sampledFields = (await realtimeStorage.get("fields")).fields;
    this.dailyFields = (await dailyStorage.get("fields")).fields;

    this.schedule(this.sampleSchedule, "getStocks", this.sampledFields, "realtime-storage")
    this.a(["getStocks", this.sampledFields, "realtime-storage"]);
    this.schedule(this.dailySchedule, "getStocks", this.dailyFields, "daily-storage")
    this.a(["getStocks", this.dailyFields, "daily-storage"]);
  }

  async getStocks(fields, storageKey){
    var workers = [];

    var db = await this.resolve(storageKey)

    for(var i = 0; i < this.concurrency; i++){
      workers.push(await nEw(StockWorker, i));
    }

    this.symbols.map((val, idx) =>{
      workers[idx%this.concurrency].a(["getStock", val, fields, db])
    });


    for(var i = 0; i < this.concurrency; i++){
      workers[i].a(["__destroy"]);
    }

  }

}

module.exports = Stocks;

class StockWorker extends BaseAsync{
  constructor(idx){
    super();
    this.idx = idx;
  }

  getStock(symbol, fields, db){
    return new Promise((res, rej) => {
      yahooFinance.snapshot({
        symbol: symbol,
        fields: fields || ['s', 'a', 'b', 'a5', 'b6', 'a2', 'd1', 'l1'],
      }, {timeout:10000}, function (err, snapshot) {
        if(err){
          rej(err);
          return;
        }
        snapshot.date = new Date().toISOString();
        db.a(["upsert", symbol, snapshot]);
        res(snapshot);
      });
    });
  }
}
