const Path = require('path');
const FS = require('fs/promises');
const Thread = require('worker_threads');

const send = (event, data) => {
	Thread.parentPort.postMessage({event, data});
};
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
const findByIndex = (index, key) => {
	if (!indexedKeys.includes(index)) return -1;
	var map = indexedContent.get(index);
	if (!map) return -1;
	var ele = map.get(key);
	if (!ele) return -1;
	return content.indexOf(ele);
};
const saveFile = async () => {
	if (!dirty) return false;
	dirty = false;
	var done = false;

	if (autoClear > 0) clearData();
	var ctx = JSON.stringify(content);
	try {
		await FS.writeFile(Thread.workerData.filepath, ctx, 'utf-8');
		done = true;
	}
	catch (err) {
		console.error(err);
		done = false;
	}

	if (dirty === true) {
		if (!!requestSaveFile.timer) {
			let list = requestSaveFile.resList.map(r => r);
			requestSaveFile.resList.splice(0, requestSaveFile.resList.length);
			clearTimeout(requestSaveFile.timer);
			requestSaveFile.timer = null;
			list.forEach(r => r(false));
		}
		dirty = false;
		requestSaveFile();
	}
	else {
		requestSaveFile.timer = null;
	}
	return done;
};
const requestSaveFile = () => new Promise(res => {
	if (dirty) return requestSaveFile.resList.push(res);
	dirty = true;
	if (!!requestSaveFile.timer) {
		let list = requestSaveFile.resList.map(r => r);
		requestSaveFile.resList.splice(0, requestSaveFile.resList.length);
		clearTimeout(requestSaveFile.timer);
		list.forEach(r => r(false));
	}
	requestSaveFile.timer = setTimeout(async () => {
		var list = requestSaveFile.resList.map(r => r);
		requestSaveFile.resList.splice(0, requestSaveFile.resList.length);
		var ok = await saveFile();
		res(ok);
		list.forEach(r => r(false));
	}, saveDelay);
});
requestSaveFile.resList = [];
const clearData = () => {
	var now = Date.now();
	var removes = content.map((item, i) => {
		var duration = now - (item.stamp || 0) - item.count * delayDuration;
		return [duration, item, i];
	}).filter(item => item[0] >= autoClear);
	var lastCount = content.length;
	removes.reverse().forEach(([duration, item, index]) => {
		content.splice(index, 1);
		indexedKeys.forEach(key => {
			var map = indexedContent.get(key);
			if (!map) return;
			map.delete(item.data[key]);
		});
	});
	var count = content.length;
	if (count < lastCount) console.log("Auto Expired " + (lastCount - count) + " items.");
};
const requestDataClearer = () => {
	if (!!requestDataClearer.timer) {
		clearTimeout(requestDataClearer.timer);
	}
	if (autoClear <= 0) return;
	requestDataClearer.timer = setTimeout(clearData, ClearDuration);
};

const ClearDuration = 1000 * 5;
const content = [], indexedKeys = [], indexedContent = new Map();
var dirty = false, saveDelay = 1000, autoClear = -1, delayDuration = 0;

Thread.parentPort.on('message', msg => {
	if (!msg || !msg.event) return;
	Thread.parentPort.emit(msg.event, msg.data);
});
Thread.parentPort.on('set', request => {
	var result = false, index, ele;
	if (!request.index || request.index === '_index') {
		index = request.key;
		ele = content[request.key];
		if (!!ele) result = true;
	}
	else {
		index = findByIndex(request.index);
		if (index >= 0) {
			ele = content[index];
			result = true;
		}
	}
	if (result) {
		let value = {
			stamp: Date.now(),
			count: (ele.count || 0) + 1,
			data: request.value
		};
		content[index] = value;
		indexedKeys.forEach(key => {
			var map = indexedContent.get(key);
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
		content.push(target);
		indexedKeys.forEach(key => {
			var value = request.value[key];
			if (!value && value !== 0) return;
			var map = indexedContent.get(key);
			if (!map) {
				map = new Map();
				indexedContent.set(key, map);
			}
			map.set(value, target);
		});
	}
	send('jobdone', {tid: request.tid, result, count: content.length});
	requestSaveFile();
});
Thread.parentPort.on('get', request => {
	var result;
	if (!request.index || request.index === '_index') {
		result = content[request.key];
	}
	else if (indexedKeys.includes(request.index)) {
		let map = indexedContent.get(request.index);
		if (!!map) {
			result = map.get(request.key);
		}
	}
	if (!result) {
		send('jobdone', {tid: request.tid, result: null});
		return;
	}
	result.count = (result.count || 0) + 1; 
	result.stamp = Date.now();
	send('jobdone', {tid: request.tid, result: result.data});
});
Thread.parentPort.on('append', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	content.push(target);
	indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = indexedContent.get(key);
		if (!map) {
			map = new Map();
			indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	send('jobdone', {tid: request.tid, result: true, count: content.length});
	requestSaveFile();
});
Thread.parentPort.on('prepend', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	content.unshift(target);
	indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = indexedContent.get(key);
		if (!map) {
			map = new Map();
			indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	send('jobdone', {tid: request.tid, result: true, count: content.length});
	requestSaveFile();
});
Thread.parentPort.on('insertBefore', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	content.splice(request.index, 0, target);
	indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = indexedContent.get(key);
		if (!map) {
			map = new Map();
			indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	send('jobdone', {tid: request.tid, result: true, count: content.length});
	requestSaveFile();
});
Thread.parentPort.on('insertAfter', request => {
	var target = {
		stamp: Date.now(),
		count: 0,
		data: request.value
	};
	content.splice(request.index + 1, 0, target);
	indexedKeys.forEach(key => {
		var value = request.value[key];
		if (!value && value !== 0) return;
		var map = indexedContent.get(key);
		if (!map) {
			map = new Map();
			indexedContent.set(key, map);
		}
		map.set(value, target);
	});
	send('jobdone', {tid: request.tid, result: true, count: content.length});
	requestSaveFile();
});
Thread.parentPort.on('findByIndex', request => {
	var result;
	if (!request.index || request.index === '_index') {
		result = request.key;
		if (!content[result]) result = -1;
	}
	else {
		result = findByIndex(request.index, request.key);
	}
	send('jobdone', {tid: request.tid, result});
});
Thread.parentPort.on('remove', request => {
	var result = false, index = -1;
	if (!request.index || request.index === '_index') {
		index = request.key;
	}
	else if (indexedKeys.includes(request.index)) {
		index = findByIndex(request.index, request.key);
	}
	if (index >= 0) {
		let ele = content[index];
		if (!!ele) {
			content.splice(index, 1);
			indexedKeys.forEach(key => {
				var map = indexedContent.get(key);
				if (!map) return;
				map.delete(ele[key]);
			});
			result = true;
		}
	}
	send('jobdone', {tid: request.tid, result, count: content.length});
	requestSaveFile();
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
		content.some((item, i) => {
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
	send('jobdone', {tid: request.tid, result: {result, err}});
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
		content.some((item, i) => {
			try {
				fun(item.data, i);
			}
			catch (e) {
				err = e.message;
				return true;
			}
		});
	}
	send('jobdone', {tid: request.tid, result: {result, err}});
});
Thread.parentPort.on('flush', async request => {
	var ok = false;
	if (request.immediate) {
		ok = await saveFile();
	}
	else {
		ok = await requestSaveFile();
	}
	send('jobdone', {tid: request.tid, result: ok});
});

(async () => {
	if (Thread.workerData.option.AutoExpire > 0) {
		autoClear = Thread.workerData.option.AutoExpire;
		if (Thread.workerData.option.RefreshPower > 0) delayDuration = Thread.workerData.option.RefreshPower;
	}

	var ok = await prepareFile(Thread.workerData.filepath);
	if (!ok) {
		send('suicide');
		return;
	}

	var ctx;
	try {
		ctx = await FS.readFile(Thread.workerData.filepath);
		ctx = ctx.toString();
		content.splice(0, content.length);
		if (ctx.length > 0) {
			content.push(...(JSON.parse(ctx)));
		}
	}
	catch (err) {
		console.error(err);
		send('suicide');
		return;
	}

	if (Thread.workerData.extension) {
		let loader = require(Thread.workerData.extension);
		if (!!loader) {
			if (loader instanceof Function) await loader();
			else if (loader.init instanceof Function) await loader.init();
			else if (loader.onInit instanceof Function) await loader.onInit();
		}
	}

	var keys = Thread.workerData.option?.keys;
	if (!!keys) {
		if (!(keys instanceof Array)) keys = [keys];
		keys = keys.filter(k => (typeof k === 'string') || (k instanceof String));

		keys.forEach(key => {
			var map = new Map();
			content.forEach(line => {
				var value = line.data[key];
				if (!value) return;
				map.set(value, line);
			});
			indexedContent.set(key, map);
			indexedKeys.push(key);
		});
	}
	if (Thread.workerData.option?.delay > 0) {
		saveDelay = Thread.workerData.option?.delay;
	}
	if (autoClear > 0) clearData();

	send('init', content.length);
}) ();