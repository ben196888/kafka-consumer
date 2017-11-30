const GROUP_ID = process.env.GROUP_ID || 'kafka-consumer-client';

const Kafka = require('node-rdkafka');
const consumer = new Kafka.KafkaConsumer({
    'group.id': GROUP_ID,
    'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready', function() {
    try {
        consumer.subscribe(['topic']);
        consumer.consume();
    } catch (err) {
        console.error('An error occurred in receiving msg.');
        console.error(err);
    }
});

consumer.on('data', function(data) {
    console.log(data.value.toString());
});

process.on('SIGINT', function() {
    console.log('Disconnect Kafka...');
    consumer.commit();
    consumer.unsubscribe(['topic']);
    consumer.disconnect();
    process.exit();
});
