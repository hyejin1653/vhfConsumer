/*let webscoketIP = process.env.WEBSOCKETIP;
let topicNm = process.env.TOPIC;
let groupNm = process.env.GROUPID;
let kafkaIP = process.env.KAFKAIP;*/
let webscoketIP = "ws://155.155.4.161:9181";
let topicNm = "tp_VHF_Packet_181";
let groupNm = "tp_181";
let kafkaIP = "155.155.4.161:9092";

//console.log(webscoketIP, topicNm, groupNm, kafkaIP);

const { Kafka } = require("kafkajs");
const protobuf = require("protobufjs");
var WebSocket = require("ws");
// var WebSocketServer = WebSocket.Server,
//   wss = new WebSocketServer({ port: 6305 });
//9101, 9102, 9103

const kafka = new Kafka({
  clientId: "my-app",
  brokers: [kafkaIP],
});

async function decodeTestMessage(buffer) {
  const root = await protobuf.load("./proto/vhf.proto");
  const testMessage = root.lookupType("vhf.VHF");
  const err = testMessage.verify(buffer);
  if (err) {
    throw err;
  }
  const message = testMessage.decode(buffer);
  return testMessage.toObject(message);
}

const consumer = kafka.consumer({
  groupId: groupNm,
});

const ws = new WebSocket(webscoketIP);
ws.onopen = () => {
  console.log("ws opened on browser");
};

let gubun = false;
let sendData = [];
const initKafka = async () => {
  console.log("start subscribe");
  await consumer.connect();
  await consumer.subscribe({
    topic: topicNm,
    fromBeginning: false,
  });
  //wss.on("connection", async (ws, request) => {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      //if (err) throw err;

      let obj = await decodeTestMessage(message.value);
      //let obj = message.value;
      console.log(obj);

      //control packet
      if (obj.datatype == 2) {
        gubun = true;
        //let payload = obj.ctrl.payload;
        //let arrPayload = payload.split(",");
        //gubun = arrPayload[3];
        //console.log(gubun);
      } else if (obj.datatype == 1) {
        //if (gubun) {
        //          console.log(obj.voice.buffer);
        //        }
        let buffer = obj.voice.buffer;
        let arrBuffer = [...buffer];
        sendData = sendData.concat(arrBuffer);
        //console.log(arrBuffer);

        if (sendData.length == 8000) {
          let sendMsg = JSON.stringify(sendData);
          sendData = [];
          if (ws.readyState === ws.OPEN) {
            ws.send(sendMsg);
          }
        }
      }

      //const obj = proto.VHF_Signal.prototype.toObject(message.value.toString());

      /*if (ws.readyState === ws.OPEN) {
        //   console.log("send");
        //console.log(sendMsg );
        let sendMsg = JSON.stringify(obj);
        ws.send(sendMsg);
      }*/
      // let deviceName = obj.deviceName;
      // let channel = obj.channel;
      // let site = obj.site;
      // let buf = obj.buffer;

      // if (buf != undefined) {
      //   buf = buf.toJSON();
      //   let meg = buf.data;
      //   let chunk = 8000;

      //   const result = [];

      //   //console.log(meg);
      //   for (index = 0; index < meg.length; index += chunk) {
      //     let tempArray;
      //     // slice() 메서드를 사용하여 특정 길이만큼 배열을 분리함
      //     tempArray = meg.slice(index, index + chunk);
      //     // 빈 배열에 특정 길이만큼 분리된 배열을 추가
      //     //result.push(tempArray);
      //     //console.log(sendMsg);
      //     let obj = {};
      //     obj["channel"] = channel;
      //     obj["deviceName"] = deviceName;
      //     obj["site"] = site;
      //     obj["data"] = tempArray;
      //     obj["offset"] = message.offset;

      //     //console.log(obj);

      //     let sendMsg = JSON.stringify(obj);
      //     //console.log(sendMsg);
      //     //wss.on("connection", async (ws, request) => {
      //     if (ws.readyState === ws.OPEN) {
      //       //   console.log("send");
      //       //console.log(sendMsg );
      //       ws.send(sendMsg);
      //     }
      //     //});
      //     //console.log(tempArray);
      //   }

      //   // console.log(
      //   //   meg.length,
      //   //   meg.length / 8000,
      //   //   Math.ceil(meg.length / 8000)
      //   // );
      // }

      //websocker 연결

      //if (ws.readyState === ws.OPEN) {
      //JSON.stringify(buf);
      //console.log(meg);

      //ws.send(meg);
      //console.log(buf);
      //ws.send(meg);
      //}
    },
    //});
  });
};

//console.log("클라이언트 접속");
initKafka();
