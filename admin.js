const {kafka} = require('./client')

async function init() {
  const admin = kafka.admin();
  console.log("Admin Connecting...");
  await admin
    .connect()
    .then(() => {
      console.log("Admin Connected Sccess!");
    })
    .catch((err) => {
      console.log("Error in Connecting Admin: ", err);
    });

  await admin
    .createTopics({
      topics: [
        {
          topic: "riders-updates",
          numPartitions: 2, //North adn south Idnai
          replicationFactor: 1,
        },
      ],
    })
    .then(() => {
      console.log("Topic Created Successfully!");
    })
    .catch((err) => {
      console.log("Error in creating topic: ", err);
    });

  await admin
    .disconnect()
    .then(() => {
      console.log("Admin Disconnected Successfully!");
    })
    .catch((err) => {
      console.log("Error in dissconnecting Admin: ", err);
    });
}

init();
