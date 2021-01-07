#include <node.h>
#include <string>
#include <cstring>
#include <sstream>
#include <vector>
#include <memory>
#include <functional>
#include <exception>

#include "hdf5V8.hpp"
#include "h5_lt.hpp"
#include "file.h"
#include "group.h"
#include "dataset.h"
#include "int64.hpp"
#include "filters.hpp"
#include "H5LTpublic.h"
#include "H5PTpublic.h"
#include "H5Lpublic.h"

namespace NodeHDF5 {

  Dataset::Dataset(hid_t id)
      : Methods(id) {
    is_open = true;
  }

  v8::Persistent<v8::Function> Dataset::Constructor;

  void Dataset::Initialize() {

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    // instantiate constructor template
    Local<FunctionTemplate> t = FunctionTemplate::New(isolate, New);

    // set properties
    t->SetClassName(String::NewFromUtf8(isolate, "Dataset", v8::NewStringType::kInternalized).ToLocalChecked());
    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->InstanceTemplate()->SetHandler(v8::NamedPropertyHandlerConfiguration(
      nullptr, nullptr, nullptr, QueryCallbackDelete));

    // member method prototypes
    setPrototypeMethod(isolate, t, v8::String::NewFromUtf8(isolate, "open", v8::NewStringType::kInternalized).ToLocalChecked(), Open);
    setPrototypeMethod(isolate, t, v8::String::NewFromUtf8(isolate, "close", v8::NewStringType::kInternalized).ToLocalChecked(), Close);
    setPrototypeMethod(isolate, t, v8::String::NewFromUtf8(isolate, "read", v8::NewStringType::kInternalized).ToLocalChecked(), Read);
    setPrototypeMethod(isolate, t, v8::String::NewFromUtf8(isolate, "refresh", v8::NewStringType::kInternalized).ToLocalChecked(), Refresh);

    // initialize constructor reference
    Constructor.Reset(v8::Isolate::GetCurrent(), t->GetFunction(context).ToLocalChecked());
  }

  void Dataset::New(const v8::FunctionCallbackInfo<Value>& args) {
    v8::Isolate* isolate = args.GetIsolate();
    v8::Local<v8::Context> context = isolate->GetCurrentContext();

    try {
      if (args.Length() == 3 && args[0]->IsString() && args[1]->IsObject()) {
        // store specified dataset name
        String::Utf8Value        group_name(isolate, args[0]->ToString(context).ToLocalChecked());
        std::vector<std::string> trail;
        std::vector<hid_t>       hidPath;
        std::istringstream       buf(*group_name);
        for (std::string token; getline(buf, token, '/');)
          if (!token.empty())
            trail.push_back(token);

        hid_t previous_hid;
        // unwrap parent object
        std::string constructorName = "File";
        if (constructorName.compare(*String::Utf8Value(isolate, args[1]->ToObject(context).ToLocalChecked()->GetConstructorName())) == 0) {

          File* parent = ObjectWrap::Unwrap<File>(args[1]->ToObject(context).ToLocalChecked());
          previous_hid = parent->getId();
        } else {
          Group* parent = ObjectWrap::Unwrap<Group>(args[1]->ToObject(context).ToLocalChecked());
          previous_hid  = parent->getId();
        }
        for (unsigned int index = 0; index < trail.size() - 1; index++) {
          // check existence of stem
          if (H5Lexists(previous_hid, trail[index].c_str(), H5P_DEFAULT)) {
            hid_t hid = H5Gopen(previous_hid, trail[index].c_str(), H5P_DEFAULT);
            if (hid >= 0) {
              if (index < trail.size() - 1)
                hidPath.push_back(hid);
              previous_hid = hid;
              continue;
            } else {
              std::string msg = "Group " + trail[index] + " doesn't exist";
              v8::Isolate::GetCurrent()->ThrowException(
                  v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), msg.c_str(), v8::NewStringType::kInternalized).ToLocalChecked()));
              args.GetReturnValue().SetUndefined();
              return;
            }
          } else {
            std::string msg = "Group " + trail[index] + " doesn't exist";
            v8::Isolate::GetCurrent()->ThrowException(
                v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), msg.c_str(), v8::NewStringType::kInternalized).ToLocalChecked()));
            args.GetReturnValue().SetUndefined();
            return;
          }
        }

        if (H5Lexists(previous_hid, trail[trail.size() - 1].c_str(), H5P_DEFAULT)) {
          // create dataset H5Dopen
          Dataset* dataset   = new Dataset(H5Dopen(previous_hid, trail[trail.size() - 1].c_str(), H5P_DEFAULT));
          dataset->gcpl_id = H5Pcreate(H5P_DATASET_CREATE);
          // herr_t err     = H5Pset_link_creation_order(dataset->gcpl_id, args[2]->IntegerValue(context).ToChecked());
          // if (err < 0) {
          //   v8::Isolate::GetCurrent()->ThrowException(
          //       v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "Failed to set link creation order", v8::NewStringType::kInternalized).ToLocalChecked()));
          //   args.GetReturnValue().SetUndefined();
          //   return;
          // }
         //        group->m_group.
          dataset->name.assign(trail[trail.size() - 1]);
          for (std::vector<hid_t>::iterator it = hidPath.begin(); it != hidPath.end(); ++it)
            dataset->hidPath.push_back(*it);
          dataset->Wrap(args.This());

          // attach various properties
          Local<Object> instance = Int64::Instantiate(args.This(), dataset->id);
          Int64*        idWrap   = ObjectWrap::Unwrap<Int64>(instance);
          idWrap->setValue(dataset->id);
          v8::Maybe<bool> ret = args.This()->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "id", v8::NewStringType::kInternalized).ToLocalChecked(), instance);
          if(ret.ToChecked()){
            
          }
        }
        else{
          Dataset* dataset   = new Dataset(-1);
          dataset->name.assign(trail[trail.size() - 1]);
          dataset->Wrap(args.This());
          // attach various properties
          Local<Object> instance = Int64::Instantiate(args.This(), dataset->id);
          Int64*        idWrap   = ObjectWrap::Unwrap<Int64>(instance);
          idWrap->setValue(dataset->id);
          v8::Maybe<bool> ret = args.This()->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "id", v8::NewStringType::kInternalized).ToLocalChecked(), instance);
          if(ret.ToChecked()){
            
          }
        }

      } else {
        Local<Object> instance = Int64::Instantiate(args.This(), -1);
        Int64*        idWrap   = ObjectWrap::Unwrap<Int64>(instance);
        idWrap->setValue(-1);
        v8::Maybe<bool> ret = args.This()->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "id", v8::NewStringType::kInternalized).ToLocalChecked(), instance);
        if(ret.ToChecked()){
          
        }
      }
    } catch (std::exception& ex) {
      v8::Isolate::GetCurrent()->ThrowException(
          v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "Group open failed", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
    } catch (...) {
      v8::Isolate::GetCurrent()->ThrowException(
          v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "Group open failed", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
    }

    return;
  }

  

  void Dataset::Open(const v8::FunctionCallbackInfo<Value>& args) {
    v8::Isolate* isolate = args.GetIsolate();
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    // fail out if arguments are not correct
    if (args.Length() != 2 || !args[0]->IsString() || !args[1]->IsObject()) {

      v8::Isolate::GetCurrent()->ThrowException(
          v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "expected name, file", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
    }

    // store specified dataset name
    String::Utf8Value dataset_name(isolate, args[0]->ToString(context).ToLocalChecked());

    std::vector<std::string> trail;
    std::vector<hid_t>       hidPath;
    std::istringstream       buf(*dataset_name);
    for (std::string token; getline(buf, token, '/');) {
      if (!token.empty()) {
        trail.push_back(token);
      }
    }

    hid_t previous_hid;

    // unwrap parent object
    std::string constructorName = "File";
    if (constructorName.compare(*String::Utf8Value(isolate, args[1]->ToObject(context).ToLocalChecked()->GetConstructorName())) == 0) {

      File* parent = ObjectWrap::Unwrap<File>(args[1]->ToObject(context).ToLocalChecked());
      previous_hid = parent->getId();
    } else {
      Group* parent = ObjectWrap::Unwrap<Group>(args[1]->ToObject(context).ToLocalChecked());
      previous_hid  = parent->getId();
    }
    for (unsigned int index = 0; index < trail.size() - 1; index++) {
      // check existence of stem
      if (H5Lexists(previous_hid, trail[index].c_str(), H5P_DEFAULT)) {
        hid_t hid = H5Gopen(previous_hid, trail[index].c_str(), H5P_DEFAULT);
        if (hid >= 0) {
          if (index < trail.size() - 1)
            hidPath.push_back(hid);
          previous_hid = hid;
          continue;
        } else {
          std::string msg = "Group" + trail[index] + " doesn't exist";
          v8::Isolate::GetCurrent()->ThrowException(
              v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), msg.c_str(), v8::NewStringType::kInternalized).ToLocalChecked()));
          args.GetReturnValue().SetUndefined();
          return;
        }
      } else {
        std::string msg = "Group" + trail[index] + " doesn't exist";
        v8::Isolate::GetCurrent()->ThrowException(v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), msg.c_str(), v8::NewStringType::kInternalized).ToLocalChecked()));
        args.GetReturnValue().SetUndefined();
        return;
      }
    }
    Dataset* dataset   = new Dataset(H5Dopen(previous_hid, trail[trail.size() - 1].c_str(), H5P_DEFAULT));
    dataset->gcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    // herr_t err     = H5Pset_link_creation_order(dataset->gcpl_id, args[2]->IntegerValue(context).ToChecked());
    // if (err < 0) {
    //   v8::Isolate::GetCurrent()->ThrowException(
    //       v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "Failed to set link creation order", v8::NewStringType::kInternalized).ToLocalChecked()));
    //   args.GetReturnValue().SetUndefined();
    //   return;
    // }
    dataset->name.assign(trail[trail.size() - 1]);
    for (std::vector<hid_t>::iterator it = hidPath.begin(); it != hidPath.end(); ++it)
      dataset->hidPath.push_back(*it);
    dataset->Wrap(args.This());
    Local<Object> instance = Int64::Instantiate(args.This(), dataset->id);
    Int64*        idWrap   = ObjectWrap::Unwrap<Int64>(instance);
    idWrap->setValue(dataset->id);

    // attach various properties
    v8::Maybe<bool> ret = args.This()->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "id", v8::NewStringType::kInternalized).ToLocalChecked(), instance);
    if(ret.ToChecked()){
      
    }
  }

  void Dataset::Read(const v8::FunctionCallbackInfo<Value>& args) {
    v8::Isolate* isolate = args.GetIsolate();
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    // fail out if arguments are not correct
    if (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) {
      v8::Isolate::GetCurrent()->ThrowException(
						v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "expected [options], callback", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;

    } else if (args.Length() == 1 && ((!args[0]->IsObject() && !args[0]->IsFunction()))) {
      v8::Isolate::GetCurrent()->ThrowException(
						v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "expected callback[options]", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;

    }
    
    /*  HERE */

    
    // unwrap dataset
    Dataset* dataset = ObjectWrap::Unwrap<Dataset>(args.This());
    hid_t did = dataset->getId();
    hid_t dataspace_id = H5Dget_space(did);
    if(dataspace_id < 0){
      v8::Isolate::GetCurrent()->ThrowException(
						v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to find dataset space id", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
      
    }
    int rank = H5Sget_simple_extent_ndims(dataspace_id); 

    bool bindAttributes = false;
    bool                       subsetOn = false;
    std::unique_ptr<hsize_t[]> start(new hsize_t[rank]);
    std::unique_ptr<hsize_t[]> stride(new hsize_t[rank]);
    std::unique_ptr<hsize_t[]> count(new hsize_t[rank]);
    if (args.Length() >= 3 && args[2]->IsObject()) {
      bindAttributes = H5lt::is_bind_attributes(args, 2);
      subsetOn=H5lt::get_dimensions(args, 2, start, stride, count, rank);
    }
    
    /* get an identifier for the datatype. */
    size_t bufSize = 0;
    H5T_class_t class_id;
    std::unique_ptr<hsize_t[]> values_dim(new hsize_t[rank]);

    hid_t t = H5Dget_type(did);
    /* get the class. */
    class_id = H5Tget_class(t);
    /* get the size. */
    bufSize = H5Tget_size(t);
    /* get dimensions */
    if(H5Sget_simple_extent_dims(dataspace_id, values_dim.get(), NULL) < 0){
      v8::Isolate::GetCurrent()->ThrowException(
						v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to find dataset info", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
    }

    if(!subsetOn){
      for(int index=0;index<rank;index++){
	count.get()[index]=values_dim.get()[index];
      }
    }

    hsize_t theSize = bufSize;
    switch (rank) {
    case 4: theSize = values_dim.get()[0] * values_dim.get()[1] * values_dim.get()[2] * values_dim.get()[3]; break;
    case 3: theSize = values_dim.get()[0] * values_dim.get()[1] * values_dim.get()[2]; break;
    case 2: theSize = values_dim.get()[0] * values_dim.get()[1]; break;
    case 1: theSize = values_dim.get()[0]; break;
    case 0: theSize = bufSize; break;
    default:
      v8::Isolate::GetCurrent()->ThrowException(
						v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "unsupported rank", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
      break;
    }
    if(subsetOn){
      theSize = 1;
      for (int rankIndex = 0; rankIndex < rank; rankIndex++) {
	theSize *= count.get()[rankIndex];
      }
    }

    hid_t type_id = H5Tget_native_type(t, H5T_DIR_ASCEND);
    hid_t memspace_id  = H5S_ALL;
    int err;
    switch (class_id) {
    case H5T_ARRAY: {
      hid_t basetype_id  = H5Tget_super(type_id);
      int                        arrayRank    = H5Tget_array_ndims(type_id);
      std::unique_ptr<hsize_t[]> arrayDims(new hsize_t[arrayRank]);
      H5Tget_array_dims(type_id, arrayDims.get());
      std::unique_ptr<char* []> vl(new char*[arrayDims.get()[0]]);
      if (!(H5Tis_variable_str(basetype_id)>0)) {
	size_t typeSize = H5Tget_size(basetype_id);
	for (unsigned int arrayIndex = 0; arrayIndex < arrayDims.get()[0]; arrayIndex++) {
	  vl.get()[arrayIndex] = new char[typeSize + 1];
	  std::memset(vl.get()[arrayIndex], 0, typeSize + 1);
	}
      }
      err = H5Dread(did, type_id, memspace_id, dataspace_id, H5P_DEFAULT, vl.get());
      if (err < 0) {
	H5Tclose(t);
	H5Tclose(type_id);
	H5Tclose(basetype_id);
	v8::Isolate::GetCurrent()->ThrowException(
						  v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to read array dataset", v8::NewStringType::kInternalized).ToLocalChecked()));
	args.GetReturnValue().SetUndefined();
	return;
      }
      hsize_t arrayStart=0;
      hsize_t arrayMaximum=values_dim.get()[0];
      if(subsetOn){
	arrayStart=start.get()[0];
	arrayMaximum=std::min(values_dim.get()[0], arrayStart+count.get()[0]);
      }
      Local<Array> array = Array::New(v8::Isolate::GetCurrent(), std::min(values_dim.get()[0], count.get()[0]));
      for (unsigned int arrayIndex = arrayStart; arrayIndex < arrayMaximum; arrayIndex++) {
	v8::Maybe<bool> ret = array->Set(context, arrayIndex-arrayStart,
					 String::NewFromUtf8(
							     v8::Isolate::GetCurrent(), vl.get()[arrayIndex], v8::NewStringType::kNormal, std::strlen(vl.get()[arrayIndex])).ToLocalChecked());
	if(ret.ToChecked()){
              
	}
      }
      args.GetReturnValue().Set(array);
      H5Tclose(t);
      H5Tclose(type_id);
      H5Tclose(basetype_id);
    } break;
    case H5T_STRING: {
      if (H5Tis_variable_str(type_id)>0) {
	hid_t basetype_id  = H5Tget_super(type_id);
	if(subsetOn){
	  std::unique_ptr<hsize_t[]> maxsize(new hsize_t[rank]);
	  for(int rankIndex=0;rankIndex<rank;rankIndex++)
	    maxsize[rankIndex]=H5S_UNLIMITED;
	  memspace_id           = H5Screate_simple(rank, count.get(), maxsize.get());
	  dataspace_id          = H5Dget_space(did);
	  herr_t err            = H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start.get(), stride.get(), count.get(), NULL);
	  if (err < 0) {
	    if (subsetOn) {
	      H5Sclose(memspace_id);
	      H5Sclose(dataspace_id);
	    }
	    v8::Isolate::GetCurrent()->ThrowException(
						      v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to select hyperslab", v8::NewStringType::kInternalized).ToLocalChecked()));
	    args.GetReturnValue().SetUndefined();
	    return;
	  }
	  theSize = 1;
	  for (int rankIndex = 0; rankIndex < rank; rankIndex++) {
	    theSize *= count.get()[rankIndex];
	  }
	}
	std::unique_ptr<char* []> tbuffer(new char*[std::min(values_dim.get()[0], theSize)]);
	err=H5Dread(did, type_id, memspace_id, dataspace_id, H5P_DEFAULT, tbuffer.get());
	Local<Array> array = Array::New(v8::Isolate::GetCurrent(), std::min(values_dim.get()[0], theSize));
	for (unsigned int arrayIndex = 0; arrayIndex < std::min(values_dim.get()[0], theSize); arrayIndex++) {
	  //std::string s(tbuffer.get()[arrayIndex]);
	  array->Set(context,
		     arrayIndex,
		     String::NewFromUtf8(
					 v8::Isolate::GetCurrent(), tbuffer.get()[arrayIndex], v8::NewStringType::kNormal, std::strlen(tbuffer.get()[arrayIndex])).ToLocalChecked()).Check();
	}
	args.GetReturnValue().Set(array);
	H5Tclose(basetype_id);
      } else if (rank > 1 && values_dim.get()[0] > 0) {
	hid_t                   dataspace_id = H5S_ALL;
	hid_t                   memspace_id  = H5S_ALL;
	size_t                  typeSize     = H5Tget_size(t);
	std::unique_ptr<char[]> tbuffer(new char[typeSize * theSize]);
	size_t                  nalloc;
	H5Tencode(type_id, NULL, &nalloc);
	H5Tencode(type_id, tbuffer.get(), &nalloc);
	H5T_str_t paddingType=H5Tget_strpad(type_id);
	err = H5Dread(did, type_id, memspace_id, dataspace_id, H5P_DEFAULT, tbuffer.get());
	if (err < 0) {
	  H5Tclose(t);
	  H5Tclose(type_id);
	  v8::Isolate::GetCurrent()->ThrowException(
						    v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to read array dataset", v8::NewStringType::kInternalized).ToLocalChecked()));
	  args.GetReturnValue().SetUndefined();
	  return;
	}
	hsize_t arrayStart=0;
	if(subsetOn){
	  arrayStart=start.get()[0];
	}
	Local<Array> array = Array::New(v8::Isolate::GetCurrent(), std::min(values_dim.get()[0], count.get()[0]));
	H5lt::fill_multi_array(array, tbuffer, values_dim, count, typeSize, arrayStart, 0, rank, paddingType);
	args.GetReturnValue().Set(array);
      } else {
	std::string buffer(bufSize*theSize + 1, 0);
	err = H5Dread(did, t, H5S_ALL, H5S_ALL, H5P_DEFAULT, (char*)buffer.c_str());
	if (err < 0) {
	  v8::Isolate::GetCurrent()->ThrowException(
						    v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to read dataset into string", v8::NewStringType::kInternalized).ToLocalChecked()));
	  args.GetReturnValue().SetUndefined();
	  return;
	}

	args.GetReturnValue().Set(String::NewFromUtf8(v8::Isolate::GetCurrent(), buffer.c_str(), v8::NewStringType::kNormal, theSize).ToLocalChecked());
      }
      H5Tclose(t);
      H5Tclose(type_id);
    } break;
    case H5T_VLEN:{
      hsize_t arrayStart=0;
      hsize_t arrayMaximum=values_dim.get()[0];
      if(subsetOn){
	arrayStart=start.get()[0];
	arrayMaximum=std::min(values_dim.get()[0], arrayStart+count.get()[0]);
      }
      std::unique_ptr<hvl_t[]> vl(new hvl_t[arrayMaximum]);
      herr_t err=H5Dread(did, type_id, memspace_id, dataspace_id, H5P_DEFAULT, (void*)vl.get());
      if (err < 0) {
	v8::Isolate::GetCurrent()->ThrowException(
						  v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to vlen read dataset", v8::NewStringType::kInternalized).ToLocalChecked()));
	args.GetReturnValue().SetUndefined();
      }
      else{
	hid_t super_type = H5Tget_super(type_id);
	hsize_t typeSize = H5Tget_size(super_type);
	Local<Array> array = Array::New(v8::Isolate::GetCurrent(), std::min(values_dim.get()[0], count.get()[0]));
	for (unsigned int arrayIndex = arrayStart; arrayIndex < arrayMaximum; arrayIndex++) {
	  if(H5Tis_variable_str(super_type)){
	    std::string s((char*)vl.get()[arrayIndex].p);
	    v8::Maybe<bool> ret = array->Set(context, arrayIndex-arrayStart,
					     String::NewFromUtf8(
								 v8::Isolate::GetCurrent(), (char*)vl.get()[arrayIndex].p, v8::NewStringType::kNormal, vl.get()[arrayIndex].len).ToLocalChecked());
	    if(ret.ToChecked()){
                
	    }
	  }
	  else if(typeSize==1){
#if NODE_VERSION_AT_LEAST(14,0,0)
	    std::unique_ptr<v8::BackingStore> backing = v8::ArrayBuffer::NewBackingStore(
											 vl.get()[arrayIndex].p, typeSize * vl.get()[arrayIndex].len, [](void*, size_t, void*){}, nullptr);
	    Local<ArrayBuffer> arrayBuffer = ArrayBuffer::New(v8::Isolate::GetCurrent(), std::move(backing));
#else
	    Local<ArrayBuffer> arrayBuffer = ArrayBuffer::New(v8::Isolate::GetCurrent(), vl.get()[arrayIndex].p, typeSize * vl.get()[arrayIndex].len);
#endif
	    array->Set(context, arrayIndex-arrayStart,
		       v8::Uint8Array::New(arrayBuffer, 0, vl.get()[arrayIndex].len)).Check();
              
	  }
	}
	args.GetReturnValue().Set(array);
      }
            
      H5Tclose(type_id);
    } break;
    default:
      Local<ArrayBuffer> arrayBuffer = ArrayBuffer::New(v8::Isolate::GetCurrent(), bufSize * theSize);
      Local<TypedArray>  buffer;
      if (class_id == H5T_FLOAT && bufSize == 8) {
	type_id = H5T_NATIVE_DOUBLE;
	buffer  = Float64Array::New(arrayBuffer, 0, theSize);
      } else if (class_id == H5T_FLOAT && bufSize == 4) {
	type_id = H5T_NATIVE_FLOAT;
	buffer  = Float32Array::New(arrayBuffer, 0, theSize);
      } else if (class_id == H5T_INTEGER && bufSize == 8) {

	type_id                    = H5Dget_type(did);
	v8::Local<v8::Object> int64Buffer = node::Buffer::New(v8::Isolate::GetCurrent(), bufSize * theSize).ToLocalChecked();
	H5Dread(did, type_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, (char*)node::Buffer::Data(int64Buffer));

	hid_t native_type_id = H5Tget_native_type(type_id, H5T_DIR_ASCEND);
	if (H5Tequal(H5T_NATIVE_LLONG, native_type_id)) {
	  int64Buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "type", v8::NewStringType::kInternalized).ToLocalChecked(),
			   Int32::New(v8::Isolate::GetCurrent(), toEnumMap[H5T_NATIVE_LLONG])).Check();
	}
	args.GetReturnValue().Set(int64Buffer);
	H5Tclose(native_type_id);
	return;

      } else if (class_id == H5T_INTEGER && bufSize == 4) {
	if (H5Tget_sign(H5Dget_type(did)) == H5T_SGN_2) {
	  type_id = H5T_NATIVE_INT;
	  buffer  = Int32Array::New(arrayBuffer, 0, theSize);
	} else {
	  type_id = H5T_NATIVE_UINT;
	  buffer  = Uint32Array::New(arrayBuffer, 0, theSize);
	}
	H5Tclose(t);
      } else if ((class_id == H5T_INTEGER || class_id == H5T_ENUM) && bufSize == 2) {
	if (H5Tget_sign(H5Dget_type(did)) == H5T_SGN_2) {
	  type_id = H5T_NATIVE_SHORT;
	  buffer  = Int16Array::New(arrayBuffer, 0, theSize);
	} else {
	  type_id = H5T_NATIVE_USHORT;
	  buffer  = Uint16Array::New(arrayBuffer, 0, theSize);
	}
	H5Tclose(t);
      } else if (class_id == H5T_INTEGER && bufSize == 1) {
	if (H5Tget_sign(H5Dget_type(did)) == H5T_SGN_2) {
	  type_id = H5T_NATIVE_INT8;
	  buffer  = Int8Array::New(arrayBuffer, 0, theSize);
	} else {
	  type_id = H5T_NATIVE_UINT8;
	  buffer  = Uint8Array::New(arrayBuffer, 0, theSize);
	}
	H5Tclose(t);
      } else {
	v8::Isolate::GetCurrent()->ThrowException(
						  v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "unsupported data type in reading", v8::NewStringType::kInternalized).ToLocalChecked()));
	args.GetReturnValue().SetUndefined();
	return;
      }

      if (subsetOn) {
	std::unique_ptr<hsize_t[]> maxsize(new hsize_t[rank]);
	for(int rankIndex=0;rankIndex<rank;rankIndex++)
	  maxsize[rankIndex]=H5S_UNLIMITED;
	memspace_id           = H5Screate_simple(rank, count.get(), maxsize.get());
	dataspace_id          = H5Dget_space(did);
	herr_t err            = H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start.get(), stride.get(), count.get(), NULL);
	if (err < 0) {
	  if (subsetOn) {
	    H5Sclose(memspace_id);
	    H5Sclose(dataspace_id);
	  }
	  v8::Isolate::GetCurrent()->ThrowException(
						    v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to select hyperslab", v8::NewStringType::kInternalized).ToLocalChecked()));
	  args.GetReturnValue().SetUndefined();
	  return;
	}
      }
#if NODE_VERSION_AT_LEAST(8,0,0)
      //v8::Local<v8::Object> buffer = node::Buffer::New(v8::Isolate::GetCurrent(), bufSize * theSize).ToLocalChecked();
      err                          = H5Dread(did, type_id, memspace_id, dataspace_id, H5P_DEFAULT, (char*)node::Buffer::Data(buffer->ToObject(context).ToLocalChecked()));
#else
      //v8::Local<v8::Object> buffer = node::Buffer::New(v8::Isolate::GetCurrent(), bufSize * theSize).ToLocalChecked();
      err                          = H5Dread(did, type_id, memspace_id, dataspace_id, H5P_DEFAULT, (char*)buffer->Buffer()->Externalize().Data());
#endif
      if (err < 0) {
	if (subsetOn) {
	  H5Sclose(memspace_id);
	  H5Sclose(dataspace_id);
	}
	H5Tclose( type_id);
	v8::Isolate::GetCurrent()->ThrowException(
						  v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "failed to read dataset", v8::NewStringType::kInternalized).ToLocalChecked()));
	args.GetReturnValue().SetUndefined();
	return;
      }
      if (subsetOn) {
	H5Sclose(memspace_id);
	H5Sclose(dataspace_id);
	for (int rankIndex = 0; rankIndex < rank; rankIndex++) {
	  values_dim.get()[rankIndex]= count.get()[rankIndex]/stride.get()[rankIndex];
	}
      }
      if ((args.Length() == 3 && args[2]->IsFunction()) || (args.Length() == 4 && args[3]->IsFunction())) {
	const unsigned               argc = 1;
	v8::Persistent<v8::Function> callback(v8::Isolate::GetCurrent(), args[args.Length()-1].As<Function>());
	v8::Local<v8::Object> options = v8::Object::New(v8::Isolate::GetCurrent());
	options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rank", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), rank)).Check();
	H5T_order_t order = H5Tget_order(type_id);
	options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "endian", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), order)).Check();
	switch (rank) {
	case 4:
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[2])).Check();
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "columns", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[3])).Check();
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "sections", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[1])).Check();
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "files", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0])).Check();
	  break;
	case 3:
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[1])).Check();
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "columns", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[2])).Check();
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "sections", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0])).Check();
	  break;
	case 2:
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0])).Check();
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "columns", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[1])).Check();
	  break;
	case 1:
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0])).Check();
	  break;
	}
	if(class_id == H5T_ENUM){
	  v8::Local<v8::Object> enumeration = v8::Object::New(v8::Isolate::GetCurrent());

	  //hid_t h = H5Dopen(dataset->getId(), *dset_name, H5P_DEFAULT);
	  hid_t h = did;
	  t = H5Dget_type(h);
	  int n=H5Tget_nmembers( t );
	  for(unsigned int i=0;i<(unsigned int)n;i++){
	    char * mname=H5Tget_member_name( t, i );
	    int idx=H5Tget_member_index(t, (const char *) mname );
	    unsigned int value;
	    H5Tget_member_value( t, idx, (void *)&value );
	    hsize_t dvalue=value;
	    enumeration->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), mname, v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), dvalue)).Check();                    
#if  H5_VERSION_GE(1,8,13)
	    H5free_memory((void *)mname);
#endif
	  }
	  H5Tclose(t);
	  options->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "enumeration", v8::NewStringType::kInternalized).ToLocalChecked(), enumeration).Check();
                
	}
	v8::Local<v8::Value> argv[1] = {options};
	v8::MaybeLocal<v8::Value> ret = v8::Local<v8::Function>::New(v8::Isolate::GetCurrent(), callback)
	  ->Call(v8::Isolate::GetCurrent()->GetCurrentContext(), v8::Null(isolate), argc, argv);
	if(ret.ToLocalChecked()->IsNumber()){
                
	}
      } else{
	v8::Maybe<bool> ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rank", v8::NewStringType::kInternalized).ToLocalChecked(), Number::New(v8::Isolate::GetCurrent(), rank));
	if(ret.ToChecked()){
              
	}
	switch (rank) {
	case 4:
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[2]));
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "columns", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[3]));
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "sections", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[1]));
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "files", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0]));
	  break;
	case 3:
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[1]));
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "columns", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[2]));
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "sections", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0]));
	  break;
	case 2:
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0]));
	  if (rank > 1)
	    ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "columns", v8::NewStringType::kInternalized).ToLocalChecked(),
			      Number::New(v8::Isolate::GetCurrent(), values_dim.get()[1]));
	  break;
	case 1:
	  ret = buffer->Set(context, String::NewFromUtf8(v8::Isolate::GetCurrent(), "rows", v8::NewStringType::kInternalized).ToLocalChecked(),
			    Number::New(v8::Isolate::GetCurrent(), values_dim.get()[0]));
	  break;
	}
      }
      if(bindAttributes){

	v8::Local<v8::Object> focus=buffer->ToObject(context).ToLocalChecked();
	refreshAttributes(focus, did);
      }
      args.GetReturnValue().Set(buffer);
      break;
    }



    /*  END  */

    
  }




  void Dataset::Refresh(const v8::FunctionCallbackInfo<Value>& args){
    if (args.Length() > 0) {

      v8::Isolate::GetCurrent()->ThrowException(
          v8::Exception::Error(v8::String::NewFromUtf8(v8::Isolate::GetCurrent(), "expected arguments", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
    }

    // unwrap dataset
    Dataset* dataset = ObjectWrap::Unwrap<Dataset>(args.This());
    herr_t err = H5Drefresh(dataset->getId());
    if (err < 0) {
      v8::Isolate::GetCurrent()->ThrowException(v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "Failed to refresh group", v8::NewStringType::kInternalized).ToLocalChecked()));
	return;
    }
    return;
  }
  

  void Dataset::Close(const v8::FunctionCallbackInfo<Value>& args) {

    // fail out if arguments are not correct
    if (args.Length() > 0) {

      v8::Isolate::GetCurrent()->ThrowException(
          v8::Exception::Error(String::NewFromUtf8(v8::Isolate::GetCurrent(), "expected arguments", v8::NewStringType::kInternalized).ToLocalChecked()));
      args.GetReturnValue().SetUndefined();
      return;
    }

    // unwrap dataset
    Dataset* dataset = ObjectWrap::Unwrap<Dataset>(args.This());
    if (!dataset->isOpen()) {
      return;
    }

    for (std::vector<hid_t>::iterator it = dataset->hidPath.begin(); it != dataset->hidPath.end(); ++it) {
      H5Gclose(*it);
    }

    H5Pclose(dataset->gcpl_id);
    herr_t err = H5Dclose(dataset->id);
    if (err < 0) {
      return;
    }

    dataset->is_open = false;
  }
  
  Local<Object> Dataset::Instantiate(Local<Object> file) {
    auto isolate = v8::Isolate::GetCurrent();
    // group name and file reference
    Local<Value> argv[1] = {file};

    // return new group instance
    return v8::Local<v8::Function>::New(isolate, Constructor)->NewInstance(isolate->GetCurrentContext(), 1, argv).ToLocalChecked();
  }

  Local<Object> Dataset::Instantiate(const char* name, Local<Object> parent, unsigned long creationOrder) {
    auto isolate = v8::Isolate::GetCurrent();
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    // group name and parent reference
    Local<Value> argv[3] = {Local<Value>::New(isolate, String::NewFromUtf8(isolate, name, v8::NewStringType::kInternalized).ToLocalChecked()), parent, Uint32::New(isolate, creationOrder)};

    Local<Object> tmp;
    Local<Value> value;
    auto instance = v8::Local<v8::Function>::New(isolate, Constructor)->NewInstance(isolate->GetCurrentContext(), 3, argv);
    bool toCheck = instance.ToLocal(&value);
    // unwrap dataset
    Dataset* dataset = ObjectWrap::Unwrap<Dataset>(value->ToObject(context).ToLocalChecked());
    if (toCheck && dataset->id>=0) {
      // return new group instance
      return instance.ToLocalChecked();
    } else {
      // return empty
      std::stringstream ss;
      ss << "Failed to read group. Group "<<(name)<< " doesn't exist.";
      throw  Exception(ss.str());
      return tmp;
    }
  }
};
