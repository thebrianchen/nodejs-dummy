const {Firestore} = require('@google-cloud/firestore');

const firestore = new Firestore();
const writer = firestore._bulkWriter({disableThrottling: true});
const NUM_LOOPS = 10;
const NUM_WRITES_PER_LOOP = 500;
const BATCH_SIZES = [10, 20, 30, 40, 50, 100, 150, 250, 500];
const document = firestore.collection('foo').doc();

async function quickstart() {
  // // Single overlapping field
  let data = {foo: 'bar'};
  // for (let batchSize of BATCH_SIZES) {
  //   writer._setMaxBatchSize(batchSize);
  //   await runOverlappingFieldsTest(batchSize, data, 'SINGLE OVERLAPPING FIELD');
  // }
  //
  // // Multiple overlapping fields
  data = generateMultiOverlappingField();
  console.log('--------------------------------------------------------------');
  for (let batchSize of BATCH_SIZES) {
    writer._setMaxBatchSize(batchSize);
    await runOverlappingFieldsTest(batchSize, data, 'MULTIPLE OVERLAPPING FIELDS');
  }

  // Single random field
  // data = generateSingleRandomField();
  // console.log('--------------------------------------------------------------');
  // for (let batchSize of BATCH_SIZES) {
  //   writer._setMaxBatchSize(batchSize);
  //   await runUniqueFieldsTest(batchSize, data, 'SINGLE RANDOM FIELD');
  // }

  // Multiple random field
  // data = generateMultiRandomFields();
  // console.log('--------------------------------------------------------------');
  // for (let batchSize of BATCH_SIZES) {
  //   writer._setMaxBatchSize(batchSize);
  //   await runUniqueFieldsTest(batchSize, data, 'MULTIPLE RANDOM FIELDS');
  // }
}

async function runOverlappingFieldsTest(batchSize, data, name) {
  return runTest(
      batchSize,
      data,
      name,
      () => writer.set(firestore.collection('coll').doc(), data)
  );
}

async function runUniqueFieldsTest(batchSize, data, name) {
  return runTest(
      batchSize,
      data,
      name,
      (i) => writer.set(firestore.collection('coll').doc(), data[i])
  );
}

async function runTest(batchSize, data, name, func) {
  console.log('--------------------' + name);
  console.log('Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_LOOP);
  let total = 0;
  for (let i = 0; i < NUM_LOOPS; i++) {
    let startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_LOOP; j++) {
      func(i).catch((err) => {
        console.log('write: ' + j + ', failed with: ', err);
      });
    }
    await writer.flush();
    let endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    total += timeElapsed
  }
  console.log('average time for ' + NUM_WRITES_PER_LOOP + ' writes ', total/NUM_LOOPS + 'ms');
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
