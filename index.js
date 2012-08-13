var mysql = require('mysql');
var util = require('util');
var events = require('events');

module.exports = {
	createStore: function(options, cb){
		var options = options || {};
		var table = options.table;
		var keyColumn = options.keyColumn;
		var valueColumn = options.valueColumn;
	
		var db = new mysql.createClient({
			database: options.database,
			user: options.user,
			password: options.password
		});
		
		db.on('error', function handleDisconnect(err){
			if (!err.fatel)
				return;
			
			if (err.code !== "PROTOCOL_CONNECTION_LOST")
				throw err;
			
			db = mysql.createConnection(db.config);
			handleDisconnect(db);
			db.connect();
		})
		
		function Store(){
			events.EventEmitter.call(this);
		}
		
		util.inherits(Store, events.EventEmitter);
		
		Store.prototype.save = function(cb){
			cb = cb || new Function;
			
			var that = this;
			var operations = 0;
			var counter = 0;
			
			for (var key in that) if (that.hasOwnProperty(key))
			{
				var value = JSON.stringify(that[key]);
				
				if (saved[key] !== value)
				{
					saved[key] = value;
					++counter;
					
					db.query("INSERT exchange_settings ("+keyColumn+", "+valueColumn+") VALUES (?, ?) ON DUPLICATE KEY UPDATE "+valueColumn+" = ?",
						[key, value, value], 
						function(err, results){
							if (err) that.emit('error', err);
							
							if (--counter === 0)
								cb();
						});
				}
			}
			
		}
		
		var store = new Store();
		var saved = {}; // Used to determine what has been saved to MySQL
		
		// Expose the data from the database through the store
		db.query("SELECT "+keyColumn+", "+valueColumn+" FROM "+table, 
			function(err, results){
				if (err) return cb(err, null);
				
				results.forEach(function(row){
					var key = row[keyColumn],
						value = row[valueColumn];
					var json = JSON.parse(value);
					store[key] = json;
					saved[key] = json;
				});
				
				cb(null, store);
			});
	}
}