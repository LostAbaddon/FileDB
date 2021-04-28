const FileDB = require('./index');

var db = new FileDB('./temp/db.json', {
	keys: 'id',
	AutoExpire: 1000 * 60,
	RefreshPower: 100,
	onInit () {
		console.log('[Init::Inside]');
	},
	onClose () {
		console.log('[Close::Inside]');
	}
});
db.onInit(async () => {
	console.log('[Init::Outside]');
	console.log('Count: ' + db.count);

	var ok;
	ok = await db.append({
		id: 0,
		text: 'append-1'
	});
	console.log('Append: ' + ok, db.count);
	ok = await db.append({
		id: 2,
		text: 'append-2'
	});
	console.log('Append: ' + ok, db.count);
	ok = await db.prepend({
		id: 100,
		text: 'prepend'
	});
	console.log('Prepend: ' + ok, db.count);
	ok = await db.insertBefore(1, {
		id: 3,
		text: 'insertBefore'
	});
	console.log('InsertBefore: ' + ok, db.count);
	ok = await db.insertAfter(1, {
		id: 1,
		text: 'insertAfter'
	});
	console.log('insertAfter: ' + ok, db.count);

	ok = await db.get(2);
	console.log('get-2: ', ok, db.count);
	ok = await db.get('id', 3);
	console.log('get[3]: ', ok, db.count);
	ok = await db.set(1, {
		id: 7,
		text: 'set-1-7'
	});
	console.log('set-1-7: ', ok, db.count);
	ok = await db.findByIndex('id', 7);
	console.log('find-7: ', ok, db.count);

	ok = await db.remove('id', 7);
	console.log('Remove-7: ' + ok, db.count);

	ok = await db.filter((item, i) => {
		return item.text.indexOf(env.part) >= 0
	}, {
		part: 'set'
	});
	console.log('filter: ', ok.err, ok.result);
	ok = await db.forEach((item, i) => {
		result = result || [];
		result[i] = item.id + env.fix + item.text;
	}, {
		fix: ": ",
	});
	console.log('forEach: ', ok.err, ok.result);

	db.flush();
	console.log('flush...');
	db.flush(true);
	console.log('flush...');

	db.terminate();
});
db.onClose(() => {
	console.log('[Close::Outside]');
});