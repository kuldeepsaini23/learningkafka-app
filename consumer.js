const { kafka } = require("./client");

async function init() {
  const consumer = kafka.consumer({ groupId: "user1" });
  // Connecting Consumer
  console.log("Consumer Connecting...");
  await consumer.connect();
  console.log("Consumer Connected Successfully!");

  // Subscribing to Topic
  await consumer.subscribe({ topics: ["riders-updates"], fromBeginning: true});

  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `[${topic}]: Partition: ${partition} | Offset: ${
          message.offset
        } | Value: ${message.value.toString()} `
      );
    },
  });

  // Disconnecting Consumer
  // await consumer.disconnect();
}

init();
