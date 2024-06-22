const mysql = require("mysql");
class MySQLBackend {
	connectionError = null;
	connection = null;

	cache = {
		tableNames: [],
		table: {}
	};

	triggers = {};

	constructor({ app, path, config }) {
		this.app = app;
		this.path = path;
		this.config = config;

		this.createPool();
		this.describeTables();
	}

	createPool() {
		this.pool = mysql.createPool({
			connectionLimit: 100,
			host: this.config["mysql.host"],
			user: this.config["mysql.user"],
			password: this.config["mysql.password"],
			database: this.config["mysql.database"],
			flags: "ANSI_QUOTES"
		});
	}

	describeTables() {
		this.pool.getConnection((err, connection) => {
			try {
				if (err) {
					throw err;
				}

				connection.query("SHOW TABLES", (err, rows) => {
					if (err) {
						return (this.connectionError = err);
					}

					this.cache.tableNames = rows.map(row => Object.values(row)[0]);
					this.cache.tableNames.forEach(tableName => {
						connection.query(`DESCRIBE ${tableName}`, (err, rows) => {
							if (err) {
								return (this.connectionError = err);
							}

							this.cache.table[tableName] = rows.map(row => Object.values(row)[0]);
						});
					});
				});
			} catch (err) {
				console.error("error connecting: " + err.stack);
			} finally {
				if (connection) connection.release();
			}
		});
	}

	async start(callback) {
		console.log("Starting...");

		await this.spawnEndPoints();

		if (typeof callback === "function") {
			callback();
		}

		console.log("Started!");
	}

	getErrorCode(method) {
		switch (method) {
			case "get":
				return 404;
			case "post":
			case "put":
			case "delete":
				return 500;
			default:
				return 501;
		}
	}

	on(event, callback) {
		if (!this.triggers[event]) {
			this.triggers[event] = [];
		}

		this.triggers[event].push(callback);
	}

	trigger(event, args) {
		if (this.triggers[event]) {
			this.triggers[event].forEach(callback => callback.call(this, args));
		}
	}

	sleep(ms) {
		return new Promise(resolve => setTimeout(resolve, ms));
	}

	async handleEndpoint(method, table, req, res) {
		const id = req.params.id;
		const body = req.body;
		const query = req.query;
		const where = req.where;

		if (!this.cache.table[table]) {
			console.log(`table ${table} scheme not ready`);

			return this.sleep(200).then(() => {
				this.handleEndpoint(method, table, req, res);
			});
		}

		try {
			const data = await this[method]({ table, id, body, query, where });
			res.status(data.length ? 204 : 200).send(data.result);
		} catch (err) {
			res.status(this.getErrorCode(method)).send(err);
		}
	}

	spawnEndPoints() {
		const handleSpawnEndPoints = resolve => {
			if (!this.cache.tableNames.length) {
				console.log(`databse scheme not ready`);

				return this.sleep(200).then(() => {
					handleSpawnEndPoints(resolve);
				});
			}

			this.cache.tableNames.forEach(table => {
				this.app.get(`${this.path}/${table}/:id`, async (req, res) => {
					this.handleEndpoint("get", table, req, res);
				});

				this.app.get(`${this.path}/${table}`, async (req, res) => {
					this.handleEndpoint("get", table, req, res);
				});

				this.app.post(`${this.path}/${table}`, async (req, res) => {
					this.handleEndpoint("post", table, req, res);
				});

				this.app.put(`${this.path}/${table}/:id`, async (req, res) => {
					this.handleEndpoint("put", table, req, res);
				});

				this.app.delete(`${this.path}/${table}/:id`, async (req, res) => {
					res.status(501).send({ err: "Method Not Implemented" });
					//this.handleEndpoint('delete', table, req, res);
				});
			});

			resolve();
		};

		return new Promise(resolve => {
			handleSpawnEndPoints(resolve);
		});
	}

	escape(value) {
		return parseFloat(value, 10).toString() === value.toString() ? parseFloat(value, 10) : mysql.escape(value, true);
	}

	handleQuery(query, _args, eventKey, array, insertId) {
		return new Promise((resolve, reject) => {
			try {
				this.pool.getConnection((err, connection) => {
					try {
						if (err) {
							throw err;
						}

						connection.query(query, _args, (err, response) => {
							if (err) {
								console.log("Query error: " + err.sqlMessage);
								return reject({ error: err.sqlMessage });
							}

							let result = typeof response.insertId !== "undefined" ? { id: insertId || response.insertId } : !response.length ? {} : array ? response : response[0];
							resolve({ result });

							this.trigger(eventKey, result);
						});
					} catch (err) {
						console.error("error connecting: " + err.stack);
						reject({ error: err });
					} finally {
						if (connection) connection.release();
					}
				});
			} catch (err) {
				reject({ error: err.message });
			}
		});
	}

	get({ table, id, query }) {
		const filters = [];
		const eventKey = `get-${table}`;

		if (id) {
			filters.push(`id=${this.escape(id)}`);
		}

		if (query && Object.keys(query).length) {
			const filterKeys = Object.keys(query).filter(key => this.cache.table[table].includes(key));

			filterKeys.forEach(key => {
				let values = query[key].toString().split(",");
				let subfilter = [];

				values.forEach(value => {
					subfilter.push(`${key} = ${this.escape(value)}`);
				});

				subfilter = `(${subfilter.join(" OR ")})`;
				filters.push(subfilter);
			});
		}

		return this.handleQuery(`SELECT * FROM ?? ${filters.length ? `WHERE ${filters.join(" AND ")}` : ``}`, [table], eventKey, typeof id === "undefined");
	}

	post({ table, body }) {
		const data = [];
		const keys = [];
		const eventKey = `post-${table}`;

		this.cache.table[table].forEach(key => {
			if (typeof body[key] !== "undefined") {
				keys.push(key);
				data.push(typeof body[key] === "object" ? JSON.stringify(body[key]) : body[key]);
			}
		});

		return this.handleQuery(`INSERT INTO ?? (??) VALUES (?)`, [table, keys, data], eventKey, false, body.id);
	}

	delete() {
		return {};
	}

	put({ table, id, where, body }) {
		const eventKey = `put-${table}`;
		const data = [];
		const filters = [];

		if (id) {
			filters.push(`id=${this.escape(id)}`);
		}

		if (where && Object.keys(where).length) {
			const filterKeys = Object.keys(where).filter(key => this.cache.table[table].includes(key));

			filterKeys.forEach(key => {
				let values = where[key].toString().split(",");
				let subfilter = [];

				values.forEach(value => {
					subfilter.push(`${key} = ${this.escape(value)}`);
				});

				subfilter = subfilter.length > 1 ? `(${subfilter.join(" OR ")})` : subfilter[0];
				filters.push(subfilter);
			});
		}

		if (!filters.length) {
			throw "Put request needs a where condition";
		}

		this.cache.table[table].forEach(key => {
			if (typeof body[key] !== "undefined") {
				data.push(`${key} = ${this.escape(body[key])}`);
			}
		});

		return this.handleQuery(`UPDATE ?? SET ${data.join(",")} ${filters.length ? `WHERE ${filters.join(" AND ")}` : ``}`, [table, id], eventKey, false, id);
	}
}

module.exports = MySQLBackend;
