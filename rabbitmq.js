const amqp = require('amqplib/callback_api');
const CLOUDAMQP_URL = "amqps://ngmlhqwf:bH6pcGeQ5hJ5N1fhJ9H9cEIQBnzSiLe6@puffin.rmq2.cloudamqp.com/ngmlhqwf";

// if the connection is closed or fails to be established at all, we will reconnect
let amqpConn = null;

const start = () => {
	amqp.connect(`${CLOUDAMQP_URL}?heartbeat=60`, (err, conn) => {
		if (err) {
			console.error("[AMQP]", err.message);
			return setTimeout(start, 1000);
		}
		conn.on("error", (err) => {
			if (err.message !== "Connection closing") {
				console.error("[AMQP] conn error", err.message);
			}
		});
		conn.on("close", () => {
			console.error("[AMQP] reconnecting");
			return setTimeout(start, 1000);
		});

		console.log("[AMQP] connected");
		amqpConn = conn;

		whenConnected();
	});
}

const whenConnected = () => {
	startPublisher();
	startWorker();
}

let pubChannel = null;
let offlinePubQueue = [];
const startPublisher = () => {
	amqpConn.createConfirmChannel((err, ch) => {
		if (closeOnErr(err)) return;
		ch.on("error", (err) => {
			console.error("[AMQP] channel error", err.message);
		});
		ch.on("close", () => {
			console.log("[AMQP] channel closed");
		});

		pubChannel = ch;
		while (true) {
			let m = offlinePubQueue.shift();
			if (!m) break;
			publish(m[0], m[1], m[2]);
		}
	});
}

// method to publish a message, will queue messages internally if the connection is down and resend later
const publish = (exchange, routingKey, content) => {
	try {
		pubChannel.publish(exchange, routingKey, content, { persistent: true },
			(err, ok) => {
				if (err) {
					console.error("[AMQP] publish", err);
					offlinePubQueue.push([exchange, routingKey, content]);
					pubChannel.connection.close();
				}
				console.log("[AMQP] Publish Ok", ok);
			});
	} catch (e) {
		console.error("[AMQP] publish", e.message);
		offlinePubQueue.push([exchange, routingKey, content]);
	}
}

// A worker that acks messages only if processed succesfully
const startWorker = () => {
	amqpConn.createChannel((err, ch) => {
		if (closeOnErr(err)) return;
		ch.on("error", (err) => {
			console.error("[AMQP] channel error", err.message);
		});
		ch.on("close", () => {
			console.log("[AMQP] channel closed");
		});
		ch.prefetch(10);
		ch.assertQueue("jobs", { durable: true }, (err, _ok) => {
			if (closeOnErr(err)) return;
			ch.consume("jobs", processMsg, { noAck: false });
			console.log("Worker is started");
		});

		function processMsg(msg) {
			work(msg, (ok) => {
				try {
					if (ok)
						ch.ack(msg);
					else
						ch.reject(msg, true);
				} catch (e) {
					closeOnErr(e);
				}
			});
		}
	});
}

const work = (msg, cb) => {
	console.log("Got msg", msg.content.toString());
	cb(true);
}

const closeOnErr = (err) => {
	if (!err) return false;
	console.error("[AMQP] error", err);
	amqpConn.close();
	return true;
}

setInterval(() => {
	publish("", "jobs", new Buffer.from("work work work"));
}, 1000);

start();