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

        this.symbols.map((val, idx) => {
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

    getStock(symbol, fields, db, storageKey) {
        return new Promise((res, rej) => {
            yahooFinance.snapshot({
                symbol: symbol,
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
                    historical(symbol, "SPY", new Date(Date.now() - 1000 * 60 * 60 * 24 * (365 / 2)).toISOString())
                        .then((beta) => {
                            snapshot.SPYbeta = beta;
                            db.a(["upsert", this.collation(symbol, snapshot.date), snapshot]);
                            console.log(snapshot)
                            res(snapshot);
                        })
                        .catch((err) => {
                          console.log(err)
                            db.a(["upsert", this.collation(symbol, snapshot.date), snapshot]);
                            res(snapshot);
                        });

                    return;
                }
                db.a(["upsert", this.collation(symbol, snapshot.date), snapshot]);
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
            symbols: [stock1, stock2],
            from: from,
            to: to || new Date().toISOString()
        }, (err, snapshot) => {
            if (err) {
                rej(err);
                return;
            }
            var stocks1 = snapshot[stock1];
            var stocks2 = snapshot[stock2];

            var s1Deltas = stocks1.reduce(function(red, s, idx) {

                if (idx === 0) {
                    return red;
                }

                red.push(percentChange(stocks1[idx - 1].close, s.close));
                return red;
            }, []);

            var s2Deltas = stocks2.reduce(function(red, s, idx) {
                if (idx === 0) {
                    return red;
                }
                red.push(percentChange(stocks2[idx - 1].close, s.close));
                return red;
            }, []);

            res(stats.sampleCovariance(s1Deltas, s2Deltas) / stats.sampleVariance(s2Deltas));
        });
    });
}
