const {Firestore} = require('@google-cloud/firestore');
const sleep = require('sleep');
const fs = require('fs');

const firestore = new Firestore();

// Global variables to configure tests.
const NUM_LOOPS = 100; // Number of loops to run.
const NUM_WRITES_PER_LOOP = 5000; // Number of document writes in each loop.
const BATCH_SIZES = [10, 20, 30, 50, 100, 250];
const NUM_ENTITIES = 100; // Number of entities in each document write.
const USE_SAME_COLLECTION = true; // true: writes all docs into the same collection.
const RUN_WRITE_BATCH_TEST = true; // true: runs BulkWriter test
const USE_RANDOM_FIELDS = false; // true: use overlapping fields
const TIMED_TEST = true; // true: perform a 30-minute benchmark test
const THROTTLING_SETTING = false;

let errored = false;
const writer = firestore.bulkWriter({throttling: THROTTLING_SETTING});
writer.onWriteError((err) => {
  errored = true;
  return true;
});

async function timedTest() {
  const [startingData, updateData] = USE_RANDOM_FIELDS ?
      [generateMultiRandomFields(), generateMultiRandomFields()] :
      [generateMultiOverlappingFields(), generateMultiOverlappingFields()];
  if (RUN_WRITE_BATCH_TEST) {
    for (const batchSize of BATCH_SIZES) {
      writer._setMaxBatchSize(batchSize);
      await runTimedWriteBatchTest(batchSize, startingData, updateData);
    }
  } else {
    for (const batchSize of BATCH_SIZES) {
      writer._setMaxBatchSize(batchSize);
      await runTimedBulkWriterTest( batchSize, startingData, updateData);
    }
  }
}

async function main() {
  const [startingData, updateData] = USE_RANDOM_FIELDS ?
      [generateMultiRandomFields(), generateMultiRandomFields()] :
      [generateMultiOverlappingFields(), generateMultiOverlappingFields()];
  if (RUN_WRITE_BATCH_TEST) {
    for (const batchSize of BATCH_SIZES) {
      writer._setMaxBatchSize(batchSize);
      await runWriteBatchTest(batchSize, startingData, updateData);
    }
  } else {
    for (const batchSize of BATCH_SIZES) {
      writer._setMaxBatchSize(batchSize);
      await runBulkWriterTest( batchSize, startingData, updateData);
    }
  }
}

async function runTimedBulkWriterTest(batchSize, startingData) {
  console.log('--------- BulkWriter Timed: Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_LOOP);
  let totalTimeElapsedMs = 0;
  let i =0;

  // Run set test
  while (totalTimeElapsedMs < 30 * 60 * 1000) {
    await sleep.sleep(1);
    const startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_LOOP; j++) {
      const collection = USE_SAME_COLLECTION ? firestore.collection('benchmark') : firestore.collection(randStr());
      const docRef = collection.doc();
      writer.set(docRef, startingData).catch((err) => {
        console.log('set: ' + j + ', failed with: ', err);
      });
    }
    await writer.flush();
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
    const pastLoopsQps = Math.round(NUM_WRITES_PER_LOOP / timeElapsed* 1000);
    const loopCurrentQps = Math.round(((i+1)* NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
    const logStr = 'Run #' + i + ': past loop qps: ' + pastLoopsQps + ', avg QPS: ' + loopCurrentQps + ', time elapsed: ' + millisToMinutesAndSeconds(totalTimeElapsedMs) + ' errored: ' + errored;
    console.log(logStr);
    fs.appendFileSync('./bw-100.txt', logStr + '\n');
    i++;
    errored = false;
  }
  const qps = Math.round((i * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average set QPS:', qps);
}

async function runTimedWriteBatchTest(batchSize, startingData, updateData) {
  console.log('--------- WriteBatch Timed: Batch size: ' + batchSize + ', numWrites: '
      + NUM_WRITES_PER_LOOP);
  let totalTimeElapsedMs = 0;
  let i =0;

  // Run set test
  while (totalTimeElapsedMs < 30 * 60 * 1000) {
    await sleep.sleep(1);
    const startTime = Date.now();
    const commits = [];
    for (let j = 0; j < NUM_WRITES_PER_LOOP / batchSize; j++) {
      const batch = firestore.batch();
      for (let k = 0; k < batchSize; k++) {
        const collection = USE_SAME_COLLECTION ? firestore.collection(
            'benchmark') : firestore.collection(randStr());
        const docRef = collection.doc();
        batch.set(docRef, startingData);
      }
      commits.push(batch.commit().catch((e) => {
        console.log('errored');
        errored = true;
      }));
    }
    await Promise.all(commits);
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
    const pastLoopsQps = Math.round(NUM_WRITES_PER_LOOP / timeElapsed* 1000);
    const loopCurrentQps = Math.round(((i+1)* NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
    const logStr = 'Run #' + i + ': past loop qps: ' + pastLoopsQps + ', avg QPS: ' + loopCurrentQps + ', time elapsed: ' + millisToMinutesAndSeconds(totalTimeElapsedMs) + ' errored: ' + errored;
    console.log(logStr);
    fs.appendFileSync('./wb-100.txt', logStr + '\n');
    i++;
    errored = false;
  }
  let qps = Math.round(
      (i * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average set QPS:', qps);
}

async function runBulkWriterTest(batchSize, startingData, updateData) {
  console.log('--------- BulkWriter: Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_LOOP);
  const docRefs = [];
  let totalTimeElapsedMs = 0;

  // Run set test
  for (let i = 0; i < NUM_LOOPS; i++) {
    await sleep.sleep(2);
    const startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_LOOP; j++) {
      const collection = USE_SAME_COLLECTION ? firestore.collection('benchmark') : firestore.collection(randStr());
      const docRef = collection.doc();
      docRefs.push(docRef);
      writer.set(docRef, startingData).catch((err) => {
        console.log('set: ' + j + ', failed with: ', err);
      });
    }
    await writer.flush();
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
  }
  const qps = Math.round((NUM_LOOPS * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average set QPS:', qps);

  // Run update test
  let operationFn = (docRef, data) => {
    writer.update(docRef, data).catch((err) => {
      console.log('update failed with: ', err);
    });
  };
  await runBulkWriterTestStep(batchSize, updateData, docRefs, operationFn);

  // Run delete test
  operationFn = (docRef) => {
    writer.delete(docRef).catch((err) => {
      console.log('delete failed with: ', err);
    });
  };
  await runBulkWriterTestStep(batchSize, {}, docRefs, operationFn);
}

async function runBulkWriterTestStep(batchSize, data, docRefs, operationFn) {
  let totalTimeElapsedMs = 0;
  let index = 0;
  for (let i = 0; i < NUM_LOOPS; i++) {
    await sleep.sleep(2);
    const startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_LOOP; j++) {
      operationFn(docRefs[index], data);
      index++;
    }
    await writer.flush();
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
  }

  const qps = Math.round((NUM_LOOPS * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average QPS:', qps);
}

async function runWriteBatchTest(batchSize, startingData, updateData) {
  console.log('--------- WriteBatch: Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_LOOP);
  const docRefs = [];
  let totalTimeElapsedMs = 0;

  // Run set test
  for (let i = 0; i < NUM_LOOPS; i++) {
    await sleep.sleep(2);
    const startTime = Date.now();
    const commits = [];
    for (let j = 0; j < NUM_WRITES_PER_LOOP / batchSize; j++) {
      const batch = firestore.batch();
      for (let k = 0; k < batchSize; k++) {
        const collection = USE_SAME_COLLECTION ? firestore.collection('benchmark') : firestore.collection(randStr());
        const docRef = collection.doc();
        docRefs.push(docRef);
        batch.set(docRef, startingData);
      }
      commits.push(batch.commit().catch((e) => {
        console.log('write batch error: ', e);
      }));
    }
    await Promise.all(commits);
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
  }
  let qps = Math.round((NUM_LOOPS * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average set QPS:', qps);

  // Run update test
  totalTimeElapsedMs = 0;
  let index = 0;
  for (let i = 0; i < NUM_LOOPS; i++) {
    await sleep.sleep(2);
    const startTime = Date.now();
    const commits = [];
    for (let j = 0; j < NUM_WRITES_PER_LOOP / batchSize; j++) {
      const batch = firestore.batch();
      for (let k = 0; k < batchSize; k++) {
        batch.update(docRefs[index], updateData);
        index++;
      }
      commits.push(batch.commit().catch((e) => {
        console.log('write batch error: ', e);
      }));
    }
    await Promise.all(commits);
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
  }
  qps = Math.round((NUM_LOOPS * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average update QPS:', qps);

  // Run delete test
  totalTimeElapsedMs = 0;
  index = 0;
  for (let i = 0; i < NUM_LOOPS; i++) {
    await sleep.sleep(2);
    const startTime = Date.now();
    const commits = [];
    for (let j = 0; j < NUM_WRITES_PER_LOOP / batchSize; j++) {
      const batch = firestore.batch();
      for (let k = 0; k < batchSize; k++) {
        batch.delete(docRefs[index]);
        index++;
      }
      commits.push(batch.commit().catch((e) => {
        console.log('write batch error: ', e);
      }));
    }
    await Promise.all(commits);
    const endTime = Date.now();
    const timeElapsed = (endTime - startTime);
    totalTimeElapsedMs += timeElapsed;
  }
  qps = Math.round((NUM_LOOPS * NUM_WRITES_PER_LOOP) / totalTimeElapsedMs * 1000);
  console.log('average delete QPS:', qps);
}

function generateMultiOverlappingFields() {
  const out = {};
  const startingIndex = Math.floor(Math.random() * 100);
  for (let i = startingIndex; i < startingIndex + NUM_ENTITIES; i++) {
    out['foo' + i] = 'foo' + i;
  }
  return out;
}

function generateMultiRandomFields() {
  const out = {};
  for (let j = 0; j < NUM_ENTITIES; j++) {
    out[randStr()] = randStr();
  }
  return out;
}

function randStr() {
  return Math.random().toString(26).substring(2, 15) + Math.random().toString(
      26).substring(2, 15);
}

function millisToMinutesAndSeconds(millis) {
  const minutes = Math.floor(millis / 60000);
  const seconds = ((millis % 60000) / 1000).toFixed(0);
  return minutes + ":" + (seconds < 10 ? '0' : '') + seconds;
}

TIMED_TEST ? timedTest() : main();
