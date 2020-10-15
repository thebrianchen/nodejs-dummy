// const {Firestore} = require('@google-cloud/firestore');
// const firestore = new Firestore();

const admin = require('firebase-admin');
const serviceAccount = require('../admin-sdk-sa.json');

admin.initializeApp({credential: admin.credential.cert(serviceAccount)});
const firestore = admin.firestore();


// Read file
const fs = require('fs');
const rawData = fs.readFileSync('./firestore_test_data.json');
const coords = JSON.parse(rawData);

async function writeData() {
  const allCoords = {
    allCoords: coords
  };
  const docSnap = await firestore.collection('allCoordinates').add(allCoords);
  const data = await docSnap.get();
  console.log('data', data.data());
}

async function readData() {
  await firestore.collection('coords').get().then(snap => {
    for(doc of snap.docs) {
      console.log('doc', doc.data());
    }
  })
}

writeData();
  // readData();
