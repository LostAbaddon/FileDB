const Path = require('path');
const EventEmitter = require('events');
const Thread = require('worker_threads');

const newID = (len=32) => {
	var result = [];
	for (let i = 0; i < len; i ++) {
		result.push(Math.floor(Math.random() * 36).toString(36));
	}
	return result.join('');
};
const copyOption = option => {
	if (option === null || option === undefined) return null;
	if (typeof option === 'function' || option instanceof Function) return null;
	if (typeof option === 'number') {
		if (isNaN(option)) return null;
		return option;
	}
	if (typeof option === 'string') return option;
	if (typeof option === 'boolean') return option;
	if (option instanceof Array) return option.map(ele => copyOption(ele));
	if (option instanceof Date) return option.getTime();
	if (option instanceof Number) return option.valueOf();
	if (option instanceof String) return option.valueOf();
	if (option instanceof Boolean) return option.valueOf();
	var result = {}, has = false;
	option = option || {};
	for (let key in option) {
		let value = copyOption(option[key]);
		if (value === null) continue;
		has = true;
		result[key] = value;
	}
	if (has) return result;
	return null;
};
const regCallback = (worker, event, res, host) => {
	var tid = 'task::' + newID();
	worker.once(tid, (...args) => {
		if (args.length < 2) res(args[0]);
		else res(args);
		if (event === 'flush') {
			if (!!args[0]) host.emit('save');
		}
		else if (!!host && !!args[0]) {
			host.emit('update', event);
		}
	});
	return tid;
};

class FileDB extends EventEmitter {
	#alive = true;
	#worker = null;
	#callbacks = new Map();
	#count = 0;
	constructor (filepath, option) {
		super();
		if (filepath[0] === '.') {
			filepath = Path.join(process.cwd(), filepath);
		}
		var opt = {};
		if (!!option) {
			let o = copyOption(option);
			if (o !== null) opt = o;
		}

		this.#worker = new Thread.Worker(Path.join(__dirname, 'worker.js'), {
			workerData: { filepath, option: opt }
		});
		this.#worker.on('message', msg => {
			if (!msg || !msg.event) return;
			if (!this.#alive) return;
			this.#worker.emit(msg.event, msg.data);
		});
		this.#worker.on('jobdone', data => {
			if (!isNaN(data.count)) this.#count = data.count;
			this.#worker.emit(data.tid, data.result);
		});
		this.#worker.on('init', (count) => {
			this.#count = count;
			this.emit('init');
		});
		this.#worker.on('suicide', () => {
			this.#alive = false;
			this.emit('close');
			this.#worker.terminate();
		});

		if (!!option) {
			if (option.onInit instanceof Function) this.onInit(option.onInit);
			if (option.onUpdate instanceof Function) this.onUpdate(option.onUpdate);
			if (option.onSave instanceof Function) this.onSave(option.onSave);
			if (option.onClose instanceof Function) this.onClose(option.onClose);
		}
	}
	get count () {
		return this.#count;
	}
	set (...args) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var index, key, value;
			if (args.length < 2) return res(false);
			if (args.length === 2) {
				index = '_index';
				key = args[0];
				value = args[1];
			}
			else {
				index = args[0];
				key = args[1];
				value = args[2];
			}

			var tid = regCallback(this.#worker, 'set', res, this);
			this.#worker.postMessage({ event: 'set', data: {tid, index, key, value} });
		});
	}
	get (...args) {
		return new Promise(res => {
			if (!this.#alive) return res(undefined);

			var index, key;
			if (args.length < 1) return res(undefined);
			if (args.length === 1) {
				index = '_index';
				key = args[0];
			}
			else {
				index = args[0];
				key = args[1];
			}

			var tid = regCallback(this.#worker, 'get', res);
			this.#worker.postMessage({ event: 'get', data: {tid, index, key} });
		});
	}
	append (value) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var tid = regCallback(this.#worker, 'append', res, this);
			this.#worker.postMessage({ event: 'append', data: {tid, value} });
		});
	}
	prepend (value) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var tid = regCallback(this.#worker, 'prepend', res, this);
			this.#worker.postMessage({ event: 'prepend', data: {tid, value} });
		});
	}
	insertBefore (index, value) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var tid = regCallback(this.#worker, 'insertBefore', res, this);
			this.#worker.postMessage({ event: 'insertBefore', data: {tid, index, value} });
		});
	}
	insertAfter (index, value) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var tid = regCallback(this.#worker, 'insertAfter', res, this);
			this.#worker.postMessage({ event: 'insertAfter', data: {tid, index, value} });
		});
	}
	remove (...args) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var index, key;
			if (args.length < 1) return res(false);
			if (args.length === 1) {
				index = '_index';
				key = args[0];
			}
			else {
				index = args[0];
				key = args[1];
			}

			var tid = regCallback(this.#worker, 'remove', res, this);
			this.#worker.postMessage({ event: 'remove', data: {tid, index, key} });
		});
	}
	findByIndex (index, key) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			var tid = regCallback(this.#worker, 'findByIndex', res);
			this.#worker.postMessage({ event: 'findByIndex', data: {tid, index, key} });
		});
	}
	filter (cb, env) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			cb = cb.toString();
			env = env || {};

			var tid = regCallback(this.#worker, 'filter', res);
			this.#worker.postMessage({ event: 'filter', data: {tid, cb, env} });
		});
	}
	forEach (cb, env) {
		return new Promise(res => {
			if (!this.#alive) return res(false);

			cb = cb.toString();
			env = env || {};

			var tid = regCallback(this.#worker, 'forEach', res);
			this.#worker.postMessage({ event: 'forEach', data: {tid, cb, env} });
		});
	}
	flush (immediate=false) {
		return new Promise(res => {
			if (!this.#alive) return res(undefined);

			var tid = regCallback(this.#worker, 'flush', res, this);
			this.#worker.postMessage({ event: 'flush', data: {tid, immediate} });
		});
	}
	async terminate () {
		await this.flush(true);
		this.#worker.emit('suicide');
	}
	onInit (cb) {
		this.on('init', cb);
	}
	onUpdate (cb) {
		this.on('update', cb);
	}
	onSave (cb) {
		this.on('save', cb);
	}
	onClose (cb) {
		this.on('close', cb);
	}
}

module.exports = FileDB;