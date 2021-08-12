//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_MAIN_H
#define TUPLEX_MAIN_H

#include <aws/lambda-runtime/runtime.h>

#include <iostream>
#include <sstream>
#include <nlohmann/json.hpp>
#include <Utils.h>

#include <Lambda.pb.h>
#include <Python.h>

// lambda main function, i.e. get a json request and return a json object
extern tuplex::messages::InvocationResponse lambda_main(aws::lambda_runtime::invocation_request const& lambda_req);
extern tuplex::messages::InvocationResponse make_exception(const std::string& message);

extern void global_init();
extern void global_cleanup();
extern bool container_reused();
extern tuplex::uniqueid_t container_id();
extern void reset_executor_setup();

#endif //TUPLEX_MAIN_H