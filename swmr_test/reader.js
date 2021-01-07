var hdf5 = require('hdf5').hdf5;
var h5lt = require('hdf5').h5lt;

var Access = require('hdf5/lib/globals').Access;
var filename = '/home/ttoullie/Documents/Code/test_hdf5_swmr/node/swmr.hdf5';

var file = new hdf5.File(filename, Access.ACC_SWMR_READ);
var dataset = file.openDataset('test_group/data');

function iteration(i) {
    var arr = dataset.read();
    dataset.refresh();
    console.log(arr)
    if (i < 30) {
	setTimeout(iteration, 1000, i+1);
    }else{
	dataset.close();
	file.close();
    }
}

function main(){
    setTimeout(iteration, 1000, 1);
}

main();
