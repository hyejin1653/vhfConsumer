// let webscoketIP = process.env.WEBSOCKETIP;
// let topicNm = process.env.TOPIC;
// let groupNm = process.env.GROUPID;
//let kafkaIP = process.env.KAFKAIP;

// let webscoketIP = "ws://155.155.4.161:9511";
// let topicNm = "tp_VHF_Packet_511";
// let groupNm = "tp_511";
// let kafkaIP = "155.155.4.161:9092";

// let webscoketIP = "ws://10.1.49.173:9182";
// let topicNm = "tp_VHF_Packet_181";
// let groupNm = "test_tp_181";
// let kafkaIP = "10.1.49.173:9092";

let webscoketIP = "ws://155.155.4.228:9511";
let webscoketIP2 = "ws://155.155.4.228:7511";
let topicNm = "tp_VHF_Packet_511";
let groupNm = "test_tp_511";
let kafkaIP = "155.155.4.227:9092";

console.log(webscoketIP, topicNm, groupNm, kafkaIP);

const { Kafka, ConfigSource } = require("kafkajs");
const protobuf = require("protobufjs");
var WebSocket = require("ws");
const SnappyJS = require("snappy");
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

//상태 모니터링
const statusWs = new WebSocket(webscoketIP2);
statusWs.onopen = () => {
  console.log("statusWs opened on browser");
};
// let openFlag = false;
// wss.on("connection", function connection(ws) {
//   console.log("connection client");
//   openFlag = true;
// });

function dateConvert(dt) {
  let tDate = new Date(Number(dt));
  //console.log(dt, tDate);
  let date = ("0" + tDate.getDate()).slice(-2);
  let month = ("0" + (tDate.getMonth() + 1)).slice(-2);
  let year = tDate.getFullYear();
  let hours =
    tDate.getHours().toString().length == 1
      ? "0" + tDate.getHours().toString()
      : tDate.getHours();
  let minutes =
    tDate.getMinutes().toString().length == 1
      ? "0" + tDate.getMinutes().toString()
      : tDate.getMinutes();
  let seconds =
    tDate.getSeconds().toString().length == 1
      ? "0" + tDate.getSeconds().toString()
      : tDate.getSeconds();

  return `${year}-${month}-${date} ${hours}:${minutes}:${seconds}`;
}

let gubun = true;
let remote,
  channel = "",
  mode = "0",
  datatype,
  voice = null;
const initKafka = async () => {
  console.log("start subscribe");
  await consumer.connect();
  await consumer.subscribe({
    topic: topicNm,
    fromBeginning: false,
    fromOffset: "latest",
  });
  //wss.on("connection", async (ws, request) => {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      //if (err) throw err;

      //console.log(message.value);
      let convertDt = dateConvert(message.timestamp);
      let obj = await decodeTestMessage(message.value);
      //let obj = message.value;
      //console.log(obj, new Date().toLocaleTimeString());
      //console.log(obj);

      datatype = obj.datatype;

      let payload;
      //console.log(obj)
      if (datatype == 2) {
        let info = obj.ctrl.payload;
        payload = Buffer.from(info, "utf8");

        console.log(info);
        let arrPayload = info.split(",");

        //console.log(arrPayload)

        remote = arrPayload[1];
        channel = arrPayload[2];
        mode = arrPayload[3];

        gubun = mode == "0" ? false : true;
      } else if (datatype == 0) {
        console.log("control");
      } else {
        let buffer = obj.voice.buffer;
        voice = Buffer.from(buffer, "utf8");

        payload = obj.voice.buffer;
        //let arrBuffer = [...buffer];
        //let sendMsg = JSON.stringify(arrBuffer);
        //voice = arrBuffer;
      }

      let sendMsg;

      /*if (gubun) {
        //console.log(remote, channel, mode, voice);
        let resultObj = {};
        //resultObj["remote"] = remote;
        resultObj["channel"] = channel;
        resultObj["mode"] = true;
        resultObj["voice"] = voice;

        sendMsg = JSON.stringify(resultObj);

        voice = null;
      } else {
        //console.log(voice);

        if (voice != null) {
          let resultObj = {};
          //resultObj["remote"] = "";
          resultObj["channel"] = "";
          resultObj["mode"] = false;
          resultObj["voice"] = [];

          sendMsg = JSON.stringify(resultObj);
        }
      }*/

      if (statusWs.readyState === statusWs.OPEN) {
        let parti = Buffer.from(partition.toString(), "utf8");
        let offset = Buffer.from(message.offset.toString(), "utf8");
        let date = Buffer.from(convertDt, "utf8");
        let compressed = SnappyJS.compressSync(payload);
        let resultBuffer = Buffer.concat([parti, offset, date, compressed]);
        statusWs.send(resultBuffer);
      }

      if (ws.readyState === ws.OPEN) {
        //   //console.log(sendMsg);
        //   let resultObj = {};
        //   //resultObj["remote"] = remote;
        //   resultObj["channel"] = channel;
        //   resultObj["mode"] = gubun;
        //   resultObj["voice"] = voice;

        if (channel == "") return;

        //   //snappy를 이용하여 buffer 처리 딜레이 방지용
        let channelBuf = Buffer.from(channel, "utf8");
        let modeBuf = Buffer.from(mode, "utf8");

        let resultBuffer;
        if (voice != null) {
          let compressed = SnappyJS.compressSync(voice);

          resultBuffer = Buffer.concat([channelBuf, modeBuf, compressed]);
        } else {
          resultBuffer = Buffer.concat([channelBuf, modeBuf]);
        }

        //mode : 2  = 음성ok, 0 = 음성 안들어옴
        //channel : 2, mode : 4, voice : 상태패킷0, 아니면 164
        //console.log(channelBuf.length, modeBuf.length, resultBuffer.length);
        //let voiceArray = new Uint8Array(voice).buffer;
        //let compressed = SnappyJS.compressSync(voice);

        //sendMsg = JSON.stringify(resultObj);
        ws.send(resultBuffer);
        voice = null;
      }

      //console.log(remote, channel, mode, datatype, voice);
      /*
      //let buffer = obj.voice.buffer;
      let buffer = message.value;
      let arrBuffer = [...buffer];
      //sendData = sendData.concat(arrBuffer);

      //if (sendData.length == 8000) {
      let sendMsg = JSON.stringify(arrBuffer);
      //sendData = [];
      // if (openFlag) {
      // wss.clients.forEach(function each(ws) {
      if (ws.readyState === ws.OPEN) {
        ws.send(sendMsg);
        //sleep(1000);
      }
      */
      // });
      // }

      //}

      /******************************************************************** */
      // //control packet
      // if (obj.datatype == 2) {
      //   //console.log(obj);
      //   let payload = obj.ctrl.payload;
      //   let arrPayload = payload.split(",");
      //   let flag = arrPayload[3];

      //   if (flag == "2") {
      //     gubun = true;
      //   } else {
      //     gubun = false;
      //   }

      //   console.log(flag, gubun);
      // }

      // //console.log(obj);

      // if (obj.datatype == 1) {
      //   //if (gubun) {
      //   //          console.log(obj.voice.buffer);
      //   //        }

      //   let buffer = obj.voice.buffer;
      //   if (gubun) {
      //     let arrBuffer = [...buffer];
      //     sendData = sendData.concat(arrBuffer);
      //   }
      //   if (sendData.length == 8000) {
      //     let sendMsg = JSON.stringify(sendData);
      //     console.log("1", sendData.length);
      //     sendData = [];
      //     if (ws.readyState === ws.OPEN) {
      //       ws.send(sendMsg);
      //       //sleep(1000);
      //     }
      //   }

      //   //sendData = [];
      //   //console.log(sendMsg);

      //   //console.log(arrBuffer);
      // }

      // if (!gubun) {
      //   if (sendData.length > 0) {
      //     //console.log("length", sendData.length);

      //     let sendMsg = JSON.stringify(sendData);
      //     console.log("2", sendData.length);
      //     sendData = [];
      //     //console.log(sendMsg);

      //     if (ws.readyState === ws.OPEN) {
      //       ws.send(sendMsg);
      //       //sleep(1000);
      //     }
      //   }
      // }

      /******************************************************************** */

      // if (!gubun) {
      //   if (sendData.length > 0) {
      //     console.log("length", sendData.length);

      //     let sendMsg = JSON.stringify(sendData);
      //     sendData = [];
      //     //console.log(sendMsg);

      //     if (ws.readyState === ws.OPEN) {
      //       ws.send(sendMsg);
      //       //sleep(1000);
      //     }
      //   }
      // }

      // if (sendData.length == 8000) {
      //   console.log("length", sendData.length);
      //   if (ws.readyState === ws.OPEN) {
      //     let sendMsg = JSON.stringify(sendData);
      //     sendData = [];
      //     ws.send(sendMsg);
      //     sleep(100);
      //   }
      // } else {
      //   if (!gubun) {
      //     if (sendData.length > 0) {
      //       console.log("length2", sendData.length);

      //       let sendMsg = JSON.stringify(sendData);
      //       sendData = [];
      //       //console.log(sendMsg);

      //       if (ws.readyState === ws.OPEN) {
      //         ws.send(sendMsg);
      //         //sleep(1000);
      //       }
      //     }
      //   }
      // }

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
