//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IGNOREOPERATOR_H
#define TUPLEX_IGNOREOPERATOR_H

#include "LogicalOperator.h"
#include "LogicalOperatorType.h"
#include "ExceptionOperator.h"

namespace tuplex {
    class IgnoreOperator : public LogicalOperator, public ExceptionOperator<IgnoreOperator> {
    public:
        //IgnoreOperator() = delete;
        virtual ~IgnoreOperator() override = default;

        IgnoreOperator(LogicalOperator* parent, const ExceptionCode& ec) : LogicalOperator(parent) {
            setSchema(this->parent()->getOutputSchema());
            setCode(ec);
        }

        inline int64_t getIgnoreID() {
            // get first NON-ignore operator id
            auto parent = this->parent();
            while(parent->type() == LogicalOperatorType::IGNORE)
                parent = parent->parent();
            return parent->getID();
        }

        LogicalOperator *clone() override {
            auto copy =  new IgnoreOperator(parent()->clone(), ecCode());
            copy->copyMembers(this);
            return copy;
        }

        std::string name() override { return "ignore"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::IGNORE; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return true; }

        Schema getInputSchema() const override { return getOutputSchema(); }
        void updateSchema() { setSchema(parent()->getOutputSchema()); }
        bool retype(const std::vector<python::Type>& rowTypes) override {
            updateSchema();
            return true;
        }
        virtual std::vector<Row> getSample(const size_t num) const override { return parent()->getSample(num); }
        std::vector<std::string> columns() const override { return parent()->columns(); }
    };
}

#endif //TUPLEX_IGNOREOPERATOR_Hs