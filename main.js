const {Firestore} = require('@google-cloud/firestore');

const firestore = new Firestore();
const writer = firestore._bulkWriter();
const NUM_LOOPS = 50;
const NUM_WRITES_PER_TEST = 300;
const BATCH_SIZES = [10, 20, 30, 40, 50, 100, 150, 200, 250, 500];

async function quickstart() {
  // Single overlapping field
  let data = {foo: 'bar'};
  for (let batchSize of BATCH_SIZES) {
    writer._setMaxBatchSize(batchSize);
    await runOverlappingFieldsTest(batchSize, data, 'SINGLE OVERLAPPING FIELD');
  }

  // Multiple overlapping fields
  data = generateMultiOverlappingField();
  for (let batchSize of BATCH_SIZES) {
    writer._setMaxBatchSize(batchSize);
    await runOverlappingFieldsTest(batchSize, data, 'MULTIPLE OVERLAPPING FIELDS');
  }

  // Multiple random field
  data = generateSingleRandomField();
  for (let batchSize of BATCH_SIZES) {
    writer._setMaxBatchSize(batchSize);
    await runUniqueFieldsTest(batchSize, data, 'SINGLE RANDOM FIELD');
  }
  // Multiple random field
  data = generateMultiRandomFields();
  for (let batchSize of BATCH_SIZES) {
    writer._setMaxBatchSize(batchSize);
    await runUniqueFieldsTest(batchSize, data, 'MULTIPLE RANDOM FIELDS');
  }
}

async function runOverlappingFieldsTest(batchSize, data, name) {
  console.log('--------------------' + name + '------------------------');
  console.log('Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_TEST);
  let total = 0;
  for (let i = 0; i < NUM_LOOPS; i++) {
    let startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_TEST; j++) {
      writer.set(firestore.collection('coll').doc(), data).catch(err => {
        console.log('failed!', err);
      });
    }
    await writer.flush();
    let endTime = Date.now();
    const timeElasped = (endTime - startTime);
    total += timeElasped
  }
  console.log('average time for ' + NUM_WRITES_PER_TEST + ' writes ', total/NUM_LOOPS + 'ms');
}

async function runUniqueFieldsTest(batchSize, data, name) {
  console.log('--------------------' + name + '------------------------');
  console.log('Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_TEST);
  let total = 0;
  for (let i = 0; i < NUM_LOOPS; i++) {
    let startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_TEST; j++) {
      writer.set(firestore.collection('coll').doc(), data[i]).catch(err => {
        console.log('failed');
      });
    }
    await writer.flush();
    let endTime = Date.now();
    const timeElasped = (endTime - startTime);
    total += timeElasped
  }
  console.log('average time for ' + NUM_WRITES_PER_TEST + ' writes ', total/NUM_LOOPS + 'ms');
}

function generateMultiOverlappingField() {
  let out = {};
  for (let i = 0; i < 50; i++) {
    out["foo" + i] = "foo" + i;
  }
  return out;
}

function generateSingleRandomField() {
  let out = [];
  for (let j = 0; j < 50; j++) {
    let obj = {};
      obj[randStr()] = randStr();
    out.push(obj);
  }
  return out;
}

function generateMultiRandomFields() {
  let out = [];
  for (let j = 0; j < 50; j++) {
    let obj = {};
    for (let i = 0; i < 50; i++) {
      obj[randStr()] = randStr();
    }
    out.push(obj);
  }
  return out;
}

function randStr() {
  return Math.random().toString(26).substring(2, 15) + Math.random().toString(
      26).substring(2, 15);
}

quickstart();
