'use strict';

(require('mocha'));
require('should');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

const hdf5Lib = require('..');
const globs   = require('../lib/globals');


async function launch_python_writer(){
    const { stdout, stderr } = await exec('python3 ./test/writer.py');
    //console.log('Python Writer/Reader stdout:', stdout);
    //console.log('Python Writer/Reader stderr:', stderr);
    return stdout;
}
async function launch_nodejs_reader(){
    await sleep(1000); // Let the python server starts
    let file = new hdf5Lib.hdf5.File('./test/examples/swmr.h5', globs.Access.ACC_SWMR_READ);
    let dataset = file.openDataset('test_group/data');

    var buffer = dataset.read();
    var arr = Array.prototype.slice.call(buffer, 0);
    var array_length = [arr.length];
    await sleep(1000); 
    for (var i = 0; i < 5; i++) {
	dataset.refresh();
	buffer = dataset.read();
	arr = Array.prototype.slice.call(buffer, 0);
	array_length.push(arr.length);
	await sleep(1000);
    }
    dataset.close();
    file.close();
    return array_length;
}

function sleep(ms) {
    return new Promise((resolve) => {
	setTimeout(resolve, ms);
    });
}   


describe("testing hdf5 dataset reading in SWMR mode ", function(){
    describe("opening hdf5 dataset ", function(){
	let file;
	before(function(done){
	    file = new hdf5Lib.hdf5.File('./test/examples/swmr-1.h5', globs.Access.ACC_SWMR_READ);
	    done();
	});

	it('should be > 0, opening from file ', function(done){
	    var dataset = file.openDataset('group1/dataset-int');
	    dataset.id.should.not.equal(-1);
	    dataset.close();
	    done();
	});

	it('should be > 0, opening from group ', function(done){
	    var group = file.openGroup('group1');
	    var dataset = group.openDataset('dataset-int');
	    dataset.id.should.not.equal(-1);
	    dataset.close();
	    group.close();
	    done();
	});

	it("catch on nonexistent dataset open try on file", function(done) {
	    try{
		var dataset = file.openDataset('nonexistingdataset');
            }
            catch(error) {
		error.message.should.equal("Failed to read group. Group nonexistingdataset doesn\'t exist.");
            }
	    done();
	});

	it("catch on nonexistent dataset open try on group", function(done) {
	    try{
		var group = file.openGroup('group1');
		var dataset = group.openDataset('nonexistingdataset');
            }
            catch(error) {
		error.message.should.equal("Failed to read group. Group nonexistingdataset doesn\'t exist.");
		group.close()
            }
	    done();
	});
	after(function(done){
	    file.close();
            done();
	});
    });

    describe("reading file while Python's process reads and writes (file already open in other processes) ", function(){
	this.timeout(10000); // 10 seconds
	this.slow(15000);

	it("should get growing array ", function(done){
	    Promise.all([
		launch_python_writer(),
		launch_nodejs_reader(),
	    ]).then(([r1, r2]) => {
		for (var i = 0; i < r2.length-1; i++) {
		    r2[i+1].should.be.greaterThan(r2[i]);
		}
		done();
	    });
	});
	after(function(done){
    	    done();
	});

    });
    
});
