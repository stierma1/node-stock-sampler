var ScheduledAsync = require("pid-async-class").ScheduledAsync;
var nEw = require("pid-async-class").nEw;
var BaseAsync = require("pid-async-class").BaseAsync;
var yahooFinance = require('yahoo-finance');
var System = require("pid-system")
var stats = require("simple-statistics");

class Stocks extends ScheduledAsync {
    constructor() {
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

    async _init() {
        var configurator = await this.resolve("configuration");
        var dailyStorage = await this.resolve("daily-storage");
        var realtimeStorage = await this.resolve("realtime-storage");
        this.symbols = (await realtimeStorage.get("symbols")).symbols;

        this.concurrency = parseInt((await configurator.getConfiguration()).realtimeConcurrency) || 1;
        this.dailySchedule = (await configurator.getConfiguration()).dailySchedule || "* 24 * * * *";
        this.sampleSchedule = (await configurator.getConfiguration()).realtimeSchedule || 900000;
        this.sampledFields = (await realtimeStorage.get("fields")).fields;
        this.dailyFields = (await dailyStorage.get("fields")).fields;

        this.schedule(this.sampleSchedule, "getStocks", [this.sampledFields, "realtime-storage"])
        this.a(["getStocks", this.sampledFields, "realtime-storage"]);
        this.schedule(this.dailySchedule, "getStocks", [this.dailyFields, "daily-storage"])
        this.a(["getStocks", this.dailyFields, "daily-storage"]);
    }

    async getStocks(fields, storageKey) {
        var workers = [];

        var db = await this.resolve(storageKey)

        for (var i = 0; i < this.concurrency; i++) {
            workers.push(await nEw(StockWorker, i));
        }

        var symbols = this.symbols.reduce(function(red, val){
          if(red[red.length - 1].length < 5){
            red[red.length - 1].push(val)
          } else{
            red.push([val]);
          }
          return red;
        }, [[]])

        symbols.map((val, idx) => {
            workers[idx % this.concurrency].a(["getStock", val, fields, db, storageKey])
        });


        for (var i = 0; i < this.concurrency; i++) {
            workers[i].a(["__destroy"]);
        }

    }

}

module.exports = Stocks;

class StockWorker extends BaseAsync {
    constructor(idx) {
        super();
        this.idx = idx;
    }

    getStock(symbols, fields, db, storageKey) {
        return new Promise((res, rej) => {
            yahooFinance.snapshot({
                symbols: symbols,
                fields: fields || ['s', 'a', 'b', 'a5', 'b6', 'a2', 'd1', 'l1'],
            }, {
                timeout: 10000
            }, (err, snapshot) => {
                if (err) {
                    rej(err);
                    return;
                }
                snapshot.date = new Date().toISOString();

                if (storageKey === "daily-storage") {
                    historical(symbols, "SPY", new Date(Date.now() - 1000 * 60 * 60 * 24 * (365 / 2)).toISOString())
                        .then((betas) => {
                            snapshot.map((snapshot) => {
                              snapshot.date = new Date().toISOString();
                              snapshot.SPYbeta = betas[snapshot.symbol];
                              db.a(["upsert", this.collation(snapshot.symbol, snapshot.date), snapshot]);
                            })

                            res(snapshot);
                        })
                        .catch((err) => {
                          snapshot.map((snapshot) => {
                            snapshot.date = new Date().toISOString();
                            db.a(["upsert", this.collation(snapshot.symbol, snapshot.date), snapshot]);
                          })
                          res(snapshot);
                        });

                    return;
                }

                snapshot.map((snapshot) => {
                  snapshot.date = new Date().toISOString();
                  db.a(["upsert", this.collation(snapshot.symbol, snapshot.date), snapshot]);
                })

                res(snapshot);
            });
        });
    }

    collation(symbol, dateStr) {
        return symbol + "/" + dateStr;
    }
}



function percentChange(s1, s2) {
    return (s2 - s1) / s1;
}

function historical(stock1, stock2, from, to) {
    return new Promise((res, rej) => {
        yahooFinance.historical({
            symbols: stock1.concat([stock2]),
            from: from,
            to: to || new Date().toISOString()
        }, (err, snapshot) => {

            if (err) {
                rej(err);
                return;
            }
            var snaps = [];

            for(var i in snapshot){
                if(i !== "SPY"){
                  snaps.push(snapshot[i]);
                }
            }

            if(stock1.filter(function(val){
              return val === "SPY"
            }).length > 0){
              snaps.push(snapshot["SPY"]);
            }

            snaps.push(snapshot["SPY"]);
            snapshot = snaps
            //console.log(snapshot)

            var sDeltas = [];
            for(var i = 0; i < snapshot.length ; i++){
              var stock = snapshot[i];
              var sDelta = stock.reduce(function(red, s, idx) {

                  if (idx === 0) {
                      return red;
                  }

                  red.push(percentChange(stock[idx - 1].close, s.close));
                  return red;
              }, []);
              sDeltas.push(sDelta);
            }

            var betas = sDeltas.map(function(s1Deltas, idx){
              if(!(snapshot[idx] && snapshot[idx][0])){
                return {symbols:undefined, beta:NaN};
              }

              var spyDelta = sDeltas[sDeltas.length - 1];

              return {symbol:snapshot[idx][0].symbol, beta:stats.sampleCovariance(s1Deltas, spyDelta) / stats.sampleVariance(spyDelta) }
            })

            betas.pop();
            var betaObj = {};
            betas.map(function(val){
              betaObj[val.symbol] = val.beta;
            })

            res(betaObj);
        });
    });
}
