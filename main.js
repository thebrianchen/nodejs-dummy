const {Firestore, DocumentReference, GrpcStatus} = require('@google-cloud/firestore');

const firestore = new Firestore();

async function quickstart() {
  // Obtain a document reference.
  const document = firestore.doc('posts/intro-to-firestore');

  // Enter new data into the document.
  await document.set({
    title: 'Welcome to Firestore',
    body: 'Hello World',
  });
  console.log('Entered new data into the document');

  // Update an existing document.
  await document.update({
    body: 'My first Firestore app',
  }).catch((err) => {
    switch(err.code) {
      case GrpcStatus.ABORTED
        return true;
    };
  });
  console.log('Updated an existing document');

  // Read the document.
  let doc = await document.get();
  console.log('Read the document');

  // Delete the document.
  await document.delete();
  console.log('Deleted the document');
}
quickstart();


var config = {
  apiKey: "AIzaSyAcq_4hUMM1efxxUne3DhlpxTo_o6YZ-pg",
  databaseURL: "https://chenbrian-new1.firebaseio.com",
  storageBucket: "chenbrian-new1.appspot.com",
  authDomain: "chenbrian-new1.firebaseapp.com",
  messagingSenderId: "555886180858",
  projectId: "chenbrian-new1"
};
// const app = firebase.initializeApp(config, 'some-name');
// await app.delete();
// console.log('app deleted!');
const app = firebase.initializeApp(config, "some-name");
var db = firebase.firestore(app);
db.settings({host: "http://localhost:8080", ssl: false});
// firebase.firestore.setLogLevel("debug");

async function main() {
  console.log(app);
  db.enablePersistence({ experimentalTabSynchronization: false });

  const writer = db.bulkWriter();

  writer.set()


  console.log("persistence enabled");
  var citiesRef = db.collection("cities");

  citiesRef.doc("SF").set({
    name: "San Francisco",
    state: "CA",
    country: "USA",
    capital: false,
    population: 860000,
    regions: ["west_coast", "norcal"]
  });
  citiesRef.doc("BJ").set({
    name: "Beijing",
    state: null,
    country: "China",
    capital: true,
    population: 21500000,
    regions: ["jingjinji", "hebei"]
  });
  console.log("added stuff to db");

  await db.collection("cities").where("name", "in", ["San Francisco"]).get().then(snapshot => {
    snapshot.forEach(doc => {
      console.log(doc.id, ' => ', doc.data());
    });
  });
}

async function shutdownAndCP() {
  console.log("clearing stuff now");
  await app.delete();
  await db._clearPersistence();
}

async function dbInteract() {
  var docRef = db.collection("cities").doc("SF");
  docRef
    .get({source:'cache'})
    .then(function(doc) {
      if (doc.exists) {
        console.log("Document data:", doc.data());
      } else {
        // doc.data() will be undefined in this case
        console.log("No such document!");
      }
    })
    .catch(function(error) {
      console.log("Error getting document:", error);
    });
}

async function listen() {
  var docRef = db.collection("cities").doc("SF");
  docRef.onSnapshot({ includeMetadataChanges: true }, function(doc) {
    console.log('data: ', doc.data());
    console.log('doc', doc);
  });
}

main();
