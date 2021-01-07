#pragma once
#include <map>

#include <v8.h>
#include <uv.h>
#include <node.h>
#include <node_object_wrap.h>
#include <string>
#include <memory>

#include "hdf5.h"
#include "hdf5_hl.h"

#include "exceptions.hpp"
#include "attributes.hpp"
#include "methods.hpp"

namespace NodeHDF5 {

  using namespace v8;
  using namespace node;

  class Dataset : public Methods {
    friend class File;
    friend class Group;
    using Attributes::name;
    using Attributes::id;
    using Attributes::gcpl_id; /* Should be dcpl_id though */
    using Attributes::Refresh;
    using Attributes::Flush;

  protected:
    std::vector<hid_t> hidPath;

  private:
    static Persistent<Function> Constructor;
    static void New(const v8::FunctionCallbackInfo<Value>& args);

    bool is_open;

  public:
    Dataset(hid_t id);

    hid_t getId() {
      return id;
    };
    bool isOpen(){
      return is_open;
    }

    
    static void          Initialize();
    static Local<Object> Instantiate(Local<Object> file);
    static Local<Object> Instantiate(const char* path, Local<Object> file, unsigned long creationOrder = 0);

    static void Open(const v8::FunctionCallbackInfo<Value>& args);
    static void Close(const v8::FunctionCallbackInfo<Value>& args);
    static void Read(const v8::FunctionCallbackInfo<Value>& args);
    static void Refresh(const v8::FunctionCallbackInfo<Value>& args);

  protected:
  };
};
