const Path = require('path');
const FS = require('fs/promises');
const Thread = require('worker_threads');

const prepareFile = async filepath => {
	var has = false;
	try {
		has = !!(await FS.stat(filepath));
	}
	catch {
		has = false;
	}
	if (has) return true;

	var pathList = filepath.split(/[\\\/]+/);
	if (pathList[0].match(/^\w+:$/)) {
		pathList[0] = pathList[0] + Path.sep;
	}
	var last = '';
	pathList = pathList.map(p => {
		p = Path.join(last, p);
		last = p;
		return p;
	});
	pathList.reverse();
	pathList.splice(0, 1);

	for (let path of pathList) {
		try {
			has = !!(await FS.stat(path));
		}
		catch {
			has = false;
		}
		if (has) continue;
		try {
			await FS.mkdir(path);
		}
		catch (err) {
			console.error(err);
			return false;
		}
	}
	await FS.writeFile(filepath, '', 'utf-8');
	return true;
};

const DB = {};
DB.content = [];
DB.indexedKeys = [];
DB.indexedContent = new Map();
DB.dirty = false;
DB.send = (event, data) => {
	Thread.parentPort.postMessage({event, data});
};
DB.findByIndex = (index, key) => {
	if (!DB.indexedKeys.includes(index)) return -1;
	var map = DB.indexedContent.get(index);
	if (!map) return -1;
	var ele = map.get(key);
	if (!ele) return -1;
	return DB.content.indexOf(ele);
};
DB.saveFile = async () => {
	if (!DB.dirty) return false;
	DB.dirty = false;
	var done = false;

	if (autoClear > 0) DB.clearData();
	var ctx = JSON.stringify(DB.content);
	try {
		await FS.writeFile(Thread.workerData.filepath, ctx, 'utf-8');
		done = true;
	}
	catch (err) {
		console.error(err);
		done = false;
	}

	if (DB.dirty === true) {
		if (!!DB.requestSaveFile.timer) {
			let list = DB.requestSaveFile.resList.map(r => r);
			DB.requestSaveFile.resList.splice(0, DB.requestSaveFile.resList.length);
			clearTimeout(DB.requestSaveFile.timer);
			DB.requestSaveFile.timer = null;
			list.forEach(r => r(false));
		}
		DB.dirty = false;
		DB.requestSaveFile();
	}
	else {
		DB.requestSaveFile.timer = null;
	}
	return done;
};
DB.requestSaveFile = () => new Promise(res => {
	if (DB.dirty) return DB.requestSaveFile.resList.push(res);
	DB.dirty = true;
	if (!!DB.requestSaveFile.timer) {
		let list = DB.requestSaveFile.resList.map(r => r);
		DB.requestSaveFile.resList.splice(0, DB.requestSaveFile.resList.length);
		clearTimeout(DB.requestSaveFile.timer);
		list.forEach(r => r(false));
	}
	DB.requestSaveFile.timer = setTimeout(async () => {
		var list = DB.requestSaveFile.resList.map(r => r);
		DB.requestSaveFile.resList.splice(0, DB.requestSaveFile.resList.length);
		var ok = await DB.saveFile();
		res(ok);
		list.forEach(r => r(false));
	}, saveDelay);
});
DB.requestSaveFile.resList = [];
DB.clearData = () => {
	var now = Date.now();
	var removes = DB.content.map((item, i) => {
		var duration = now - (item.stamp || 0) - item.count * delayDuration;
		return [duration, item, i];
	}).filter(item => item[0] >= autoClear);
	var lastCount = DB.content.length;
	removes.reverse().forEach(([duration, item, index]) => {
		DB.content.splice(index, 1);
		DB.indexedKeys.forEach(key => {
			var map = DB.indexedContent.get(key);
			if (!map) return;
			map.delete(item.data[key]);
		});
	});
	var count = DB.content.length;
	if (count < lastCount) console.log("Auto Expired " + (lastCount - count) + " items.");
	DB.requestDataClearer();
};
DB.requestDataClearer = () => {
	if (!!DB.requestDataClearer.timer) {
		clearTimeout(DB.requestDataClearer.timer);
	}
	if (autoClear <= 0) return;
	DB.requestDataClearer.timer = setTimeout(DB.clearData, ClearDuration);
};

var ClearDuration = 1000 * 5, saveDelay = 1000, autoClear = -1, delayDuration = 0;

Thread.parentPort.on('message', msg => {
	if (!msg || !msg.event) return;
	Thread.parentPort.emit(msg.event, msg.data);
});
Thread.parentPort.on('set', request => {
	var result = false, index, ele;
	if (!request.index || request.index === '_index') {
		index = request.key;
		ele = DB.content[request.key];
		if (!!ele) result = true;
	}
	else {
		index = DB.findByIndex(request.index);
		if (index >= 0) {
			ele = DB.content[index];
			result = true;
		}
	}
	if (result) {
		let value = {
			stamp: Date.now(),
			count: (ele.count || 0) + 1,
			data: request.value
		};
		DB.content[index] = value;
		DB.indexedKeys.forEach(key => {
			var map = DB.indexedContent.get(key);
			if (!map) return;
			var v = ele.data[key];
			if (!v) return;
			map.delete(v);
			v = request.value[key];
			if (!v) return;
			map.set(v, value);
		});
	}
	else {
		let target = {
			stamp: Date.now(),
			count: 0,
			data: request.value
		};
		DB.content.push(target);
		DB.indexedKeys.forEach(key => {
			var value = request.value[key];
			if (!value && value !== 0) return;
			var map = DB.indexedContent.get(key);
			if (!map) {
				map = new Map();
				DB.indexedContent.set(key, map);
			}
			map.set(value, target);
		});
	}
	DB.send('jobdone', {tid: request.tid, result, count: DB.content.length});
	DB.requestSaveFile();
});
Thread.parentPort.on('get', request => {
	var result;
	if (!request.index || request.index === '_index') {
		result = DB.content[request.key];
	}
	else if (DB.indexedKeys.includes(request.index)) {
		let map = DB.indexedContent.get(request.index);
		if (!!map) {
			result = map.get(request.key);
		}
	}
	if (!result) {
		DB.send('jobdone', {tid: request.tid, result: null});
		return;
	}
	result.count = (result.count || 0) + 1; 
	result.stamp = Date.now();
	DB.send('jobdone', {tid: request.tid, result: result.data});
});
Thread.parentPort.on('append', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	DB.content.push(target);
	DB.indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = DB.indexedContent.get(key);
		if (!map) {
			map = new Map();
			DB.indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	DB.send('jobdone', {tid: request.tid, result: true, count: DB.content.length});
	DB.requestSaveFile();
});
Thread.parentPort.on('prepend', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	DB.content.unshift(target);
	DB.indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = DB.indexedContent.get(key);
		if (!map) {
			map = new Map();
			DB.indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	DB.send('jobdone', {tid: request.tid, result: true, count: DB.content.length});
	DB.requestSaveFile();
});
Thread.parentPort.on('insertBefore', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	DB.content.splice(request.index, 0, target);
	DB.indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = DB.indexedContent.get(key);
		if (!map) {
			map = new Map();
			DB.indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	DB.send('jobdone', {tid: request.tid, result: true, count: DB.content.length});
	DB.requestSaveFile();
});
Thread.parentPort.on('insertAfter', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	DB.content.splice(request.index + 1, 0, target);
	DB.indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = DB.indexedContent.get(key);
		if (!map) {
			map = new Map();
			DB.indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	DB.send('jobdone', {tid: request.tid, result: true, count: DB.content.length});
	DB.requestSaveFile();
});
Thread.parentPort.on('findByIndex', request => {
	var result;
	if (!request.index || request.index === '_index') {
		result = request.key;
		if (!DB.content[result]) result = -1;
	}
	else {
		result = DB.findByIndex(request.index, request.key);
	}
	DB.send('jobdone', {tid: request.tid, result});
});
Thread.parentPort.on('remove', request => {
	var result = false, index = -1;
	if (!request.index || request.index === '_index') {
		index = request.key;
	}
	else if (DB.indexedKeys.includes(request.index)) {
		index = DB.findByIndex(request.index, request.key);
	}
	if (index >= 0) {
		let ele = DB.content[index];
		if (!!ele) {
			DB.content.splice(index, 1);
			DB.indexedKeys.forEach(key => {
				var map = DB.indexedContent.get(key);
				if (!map) return;
				map.delete(ele[key]);
			});
			result = true;
		}
	}
	DB.send('jobdone', {tid: request.tid, result, count: DB.content.length});
	DB.requestSaveFile();
});
Thread.parentPort.on('filter', request => {
	var fun, err, result;
	var env = request.env;
	try {
		fun = eval(request.cb);
	}
	catch (e) {
		err = e.message;
	}
	if (!err) {
		result = [];
		DB.content.some((item, i) => {
			var available;
			try {
				available = fun(item.data, i);
			}
			catch (e) {
				err = e.message;
				return true;
			}
			if (!!available) result.push([item.data, i]);
		});
		if (!!err) result = undefined;
	}
	DB.send('jobdone', {tid: request.tid, result: {result, err}});
});
Thread.parentPort.on('forEach', request => {
	var fun, err, result;
	var env = request.env;
	try {
		fun = eval(request.cb);
	}
	catch (e) {
		err = e.message;
	}
	if (!err) {
		DB.content.some((item, i) => {
			try {
				fun(item.data, i);
			}
			catch (e) {
				err = e.message;
				return true;
			}
		});
	}
	DB.send('jobdone', {tid: request.tid, result: {result, err}});
});
Thread.parentPort.on('flush', async request => {
	var ok = false;
	if (request.immediate) {
		ok = await DB.saveFile();
	}
	else {
		ok = await DB.requestSaveFile();
	}
	DB.send('jobdone', {tid: request.tid, result: ok});
});

(async () => {
	if (Thread.workerData.option.AutoExpire > 0) {
		autoClear = Thread.workerData.option.AutoExpire;
		if (Thread.workerData.option.RefreshPower > 0) delayDuration = Thread.workerData.option.RefreshPower;
	}

	var ok = await prepareFile(Thread.workerData.filepath);
	if (!ok) {
		DB.send('suicide');
		return;
	}

	var ctx;
	try {
		ctx = await FS.readFile(Thread.workerData.filepath);
		ctx = ctx.toString();
		DB.content.splice(0, DB.content.length);
		if (ctx.length > 0) {
			DB.content.push(...(JSON.parse(ctx)));
		}
	}
	catch (err) {
		console.error(err);
		DB.send('suicide');
		return;
	}

	var keys = Thread.workerData.option?.keys;
	if (!!keys) {
		if (!(keys instanceof Array)) keys = [keys];
		keys = keys.filter(k => (typeof k === 'string') || (k instanceof String));

		keys.forEach(key => {
			var map = new Map();
			DB.content.forEach(line => {
				var value = line.data[key];
				if (!value) return;
				map.set(value, line);
			});
			DB.indexedContent.set(key, map);
			DB.indexedKeys.push(key);
		});
	}
	if (Thread.workerData.option?.delay > 0) {
		saveDelay = Thread.workerData.option?.delay;
	}
	if (autoClear > 0) DB.clearData();

	if (Thread.workerData.option.extension) {
		let loader = require(Thread.workerData.option.extension);
		if (!!loader) {
			if (loader instanceof Function) await loader(DB);
			else if (loader.init instanceof Function) await loader.init(DB);
			else if (loader.onInit instanceof Function) await loader.onInit(DB);
		}
	}

	DB.send('init', DB.content.length);
}) ();