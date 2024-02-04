const { kafka } = require("./client");

async function init() {
  const producer = kafka.producer();
  console.log("Producer Connecting...");
  await producer
    .connect()
    .then(() => {
      console.log("Producer Connected Successfully!");
    })
    .catch((err) => {
      console.log("Error in Connecting Producer: ", err);
    });

  // Sending Messages
  await producer
    .send({
      topic: "riders-updates",
      messages: [
        {
          partition: 0,
          key: "location-updates",
          value: JSON.stringify({ naem: "Tony Stark", loc: "SOUTH" }),
        },
        {
          partition: 0,
          key: "location-updates",
          value: JSON.stringify({ naem: "Kuldeep Saini", loc: "Mumbai" }),
        },
        {
          partition: 0,
          key: "location-updates",
          value: JSON.stringify({ naem: "Captain America", loc: "Delhi" }),
        },
      ],
    })
    .then((data) => {
      console.log("Message Send successfully: ", data);
    })
    .catch((err) => {
      console.log("Error in sending message: ", err);
    });

  // Disconnecting Producer
  await producer
    .disconnect()
    .then(() => {
      console.log("Producer Disconnected Successfully!");
    })
    .catch((err) => {
      console.log("Error in dissconnecting Producer: ", err);
    });
}

init(); 