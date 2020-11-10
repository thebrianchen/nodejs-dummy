const {Firestore} = require('@google-cloud/firestore');
const sleep = require('sleep');

const firestore = new Firestore();

// Global variables to configure tests.
const NUM_LOOPS = 10; // Number of loops to run.
const NUM_WRITES_PER_LOOP = 5000; // Number of document writes in each loop.
const BATCH_SIZES = [10, 20, 30, 50, 100, 250];
const NUM_ENTITIES = 50; // Number of entities in each document write.
const RUN_WRITE_BATCH_TEST = false; // true: runs BulkWriter test
const USE_RANDOM_FIELDS = false; // true: use overlapping fields
const THROTTLING_SETTING = false;

const writer = firestore.bulkWriter({throttling: THROTTLING_SETTING});
writer.onWriteError((err) => {
  console.log('write failed', err.message);
  console.log('Num failed attempts: ', err.failedAttempts);
  return true;
});

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

async function runBulkWriterTest(batchSize, startingData, updateData) {
  console.log('--------- BulkWriter: Batch size: ' + batchSize + ', numWrites: ' + NUM_WRITES_PER_LOOP);
  const docRefs = [];
  let totalTimeElapsedMs = 0;

  // Run set test
  for (let i = 0; i < NUM_LOOPS; i++) {
    await sleep.sleep(15);
    const startTime = Date.now();
    for (let j = 0; j < NUM_WRITES_PER_LOOP; j++) {
      const docRef = firestore.collection(randStr()).doc();
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
    await sleep.sleep(15);
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
        const docRef = firestore.collection(randStr()).doc();
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

main().then(()=> console.log('completed'));
