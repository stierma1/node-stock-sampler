var PouchDB = require("pouchdb");
var BaseAsync = require("pid-async-class").BaseAsync;

class Storage extends BaseAsync {
    constructor(storageKey) {
        super()
        this.storageKey = storageKey;
        this.__POUCHDB = PouchDB;
        this.pouchDB = new this.__POUCHDB(this.storageKey);
    }

    async get(id) {
        return this.pouchDB.get(id)
    }

    async upsert(id, doc) {

        var returnDoc = await (this.get(id).catch((err) => {}));

        var upload = {};
        for (var i in doc) {
            upload[i] = doc[i];
        }

        if (returnDoc) {
            return await this.pouchDB.put({
                _id: returnDoc._id,
                _rev: returnDoc._rev,
                body: upload
            })
        } else {
            return await this.pouchDB.put({
                _id: id,
                body: upload
            });
        }
    }

    async update(id, deltaAdds, deltaSubtracts) {
        var returnDoc = await this.get(id);

        if (returnDoc) {
            var upload = {};

            for (var i in returnDoc) {
                upload[i] = returnDoc[i];
            }

            for (var i in deltaSubtracts) {
                delete upload.body[i]
            }

            for (var i in deltaAdds) {
                upload.body[i] = deltaAdds[i];
            }

            return await pouchDB.put({
                _id: returnDoc._id,
                _rev: returnDoc._rev,
                body: upload
            })
        } else {
            throw new Error("No Document Found with id: " + id);
        }
    }
}

module.exports = Storage;
