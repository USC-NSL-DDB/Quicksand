// Hossein Moein
// September 12, 2017
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <DataFrame/DataFrame.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <future>
#include <random>

#include <nu/sharded_sorter.hpp>

// ----------------------------------------------------------------------------

namespace hmdf
{

// Notice memeber variables are initialized twice, but that's cheap
//
template<typename I, typename H>
DataFrame<I, H>::DataFrame(const DataFrame &that)  { *this = that; }

template<typename I, typename H>
DataFrame<I, H>::DataFrame(DataFrame &&that)  { *this = std::move(that); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
DataFrame<I, H> &
DataFrame<I, H>::operator= (const DataFrame &that)  {

    if (this != &that)  {
        indices_ = that.indices_;
        column_tb_ = that.column_tb_;
        column_list_ = that.column_list_;

        const SpinGuard guard(lock_);

        data_ = that.data_;
    }
    return (*this);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
DataFrame<I, H> &
DataFrame<I, H>::operator= (DataFrame &&that)  {

    if (this != &that)  {
        indices_ = std::exchange(that.indices_,
                                 nu_make_sharded_vector<IndexType>());
        column_tb_ = std::exchange(that.column_tb_, ColNameDict { });
        column_list_ = std::exchange(that.column_list_, ColNameList { });

        const SpinGuard guard(lock_);

        data_ = std::exchange(that.data_, DataVecVec { });
    }
    return (*this);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
DataFrame<I, H>::~DataFrame()  {

    const SpinGuard guard(lock_);

    data_.clear();
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::empty() const noexcept  { return (indices_.empty()); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::
shapeless() const noexcept  { return (empty() && column_list_.empty()); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::set_lock (SpinLock *sl)  { lock_ = sl; }

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::remove_lock ()  { lock_ = nullptr; }

// ----------------------------------------------------------------------------


template<typename I, typename H>
template<typename CF, typename ... Ts>
void DataFrame<I, H>::sort_common_(DataFrame<I, H> &df, CF &&comp_func)  {

    const size_type         idx_s = df.indices_.size();
    std::vector<size_type>  sorting_idxs(idx_s, 0);

    std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);
    std::sort(sorting_idxs.begin(), sorting_idxs.end(), comp_func);

    {
        sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);
        const SpinGuard         guard(lock_);

        for (const auto &iter : df.data_)
            iter.change(functor);
    }
    _sort_by_sorted_index_(df.indices_, sorting_idxs, idx_s);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void
DataFrame<I, H>::shuffle(const std::vector<const char *> &col_names,
                         bool also_shuffle_index)  {

    if (also_shuffle_index)  {
        std::random_device  rd;
        std::mt19937        g(rd());

        std::shuffle(indices_.begin(), indices_.end(), g);
    }

    shuffle_functor_<Ts ...>    functor;

    for (auto name_citer : col_names)  {
        const auto  citer = column_tb_.find (name_citer);

        if (citer == column_tb_.end())  {
            char buffer [512];

            sprintf(buffer,
                    "DataFrame::shuffle(): ERROR: Cannot find column '%s'",
                    name_citer);
            throw ColNotFound(buffer);
        }

        const SpinGuard guard(lock_);

        data_[citer->second].change(functor);
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
fill_missing_value_(ColumnVecType<T> &vec,
                    const T &value,
                    int limit,
                    size_type col_num)  {

    const size_type vec_size = vec.size();
    int             count = 0;

    if (limit < 0)
        vec.reserve(col_num);
    for (size_type i = 0; i < col_num; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (i >= vec_size)  {
            vec.push_back(value);
            count += 1;
        }
        else if (is_nan<T>(vec[i]))  {
            vec[i] = value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
fill_missing_ffill_(ColumnVecType<T> &vec, int limit, size_type col_num)  {

    const size_type vec_size = vec.size();

    if (vec_size == 0)  return;

    int count = 0;
    T   last_value = vec[0];

    for (size_type i = 1; i < col_num; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (i >= vec_size)  {
            if (! is_nan(last_value))  {
                vec.reserve(col_num);
                vec.push_back(last_value);
                count += 1;
            }
            else  break;
        }
        else  {
            if (! is_nan<T>(vec[i]))
                last_value = vec[i];
            else if (! is_nan<T>(last_value))  {
                vec[i] = last_value;
                count += 1;
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T,
         typename std::enable_if<! std::is_arithmetic<T>::value ||
                                 ! std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::
fill_missing_midpoint_(ColumnVecType<T> &, int, size_type)  {

    throw NotFeasible("fill_missing_midpoint_(): ERROR: Mid-point filling is "
                      "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T,
         typename std::enable_if<std::is_arithmetic<T>::value &&
                                 std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::
fill_missing_midpoint_(ColumnVecType<T> &vec, int limit, size_type)  {

    const size_type vec_size = vec.size();

    if (vec_size < 3)  return;

    int count = 0;
    T   last_value = vec[0];

    for (size_type i = 1; i < vec_size - 1; ++i)  {
        if (limit >= 0 && count >= limit)  break;

        if (! is_nan<T>(vec[i]))
            last_value = vec[i];
        else if (! is_nan<T>(last_value))  {
            for (size_type j = i + 1; j < vec_size; ++j)  {
                if (! is_nan<T>(vec[j]))  {
                    vec[i] = (last_value + vec[j]) / T(2);
                    last_value = vec[i];
                    count += 1;
                    break;
                }
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
fill_missing_bfill_(ColumnVecType<T> &vec, int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size == 0)  return;

    int count = 0;
    T   last_value = vec[vec_size - 1];

    for (long i = vec_size - 1; i >= 0; --i)  {
        if (limit >= 0 && count >= limit)  break;
        if (! is_nan<T>(vec[i]))  last_value = vec[i];
        if (is_nan<T>(vec[i]) && ! is_nan<T>(last_value))  {
            vec[i] = last_value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T,
         typename std::enable_if<! std::is_arithmetic<T>::value ||
                                 ! std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::
fill_missing_linter_(ColumnVecType<T> &, const IndexVecType &, int)  {

    throw NotFeasible("fill_missing_linter_(): ERROR: Interpolation is "
                      "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T,
         typename std::enable_if<std::is_arithmetic<T>::value &&
                                 std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::
fill_missing_linter_(ColumnVecType<T> &vec,
                     const IndexVecType &index,
                     int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size < 3)  return;

    int             count = 0;
    T               *y1 = &(vec[0]);
    T               *y2 = &(vec[2]);
    const IndexType *x = &(index[1]);
    const IndexType *x1 = &(index[0]);
    const IndexType *x2 = &(index[2]);

    for (long i = 1; i < vec_size - 1; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (is_nan<T>(vec[i]))  {
            if (is_nan<T>(*y2))  {
                bool    found = false;

                for (long j = i + 1; j < vec_size; ++j)  {
                    if (! is_nan(vec[j]))  {
                        y2 = &(vec[j]);
                        x2 = &(index[j]);
                        found = true;
                        break;
                    }
                }
                if (! found)  break;
            }
            if (is_nan<T>(*y1))  {
                for (long j = i - 1; j >= 0; --j)  {
                    if (! is_nan(vec[j]))  {
                        y1 = &(vec[j]);
                        x1 = &(index[j]);
                        break;
                    }
                }
            }
            vec[i] =
                *y1 +
                (static_cast<T>(*x - *x1) / static_cast<T>(*x2 - *x1)) *
                (*y2 - *y1);
            count += 1;
        }
        if (i < (vec_size - 2))  {
            y1 = &(vec[i]);
            y2 = &(vec[i + 2]);
            x = &(index[i + 1]);
            x1 = &(index[i]);
            x2 = &(index[i + 2]);
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
fill_missing(const std::vector<const char *> &col_names,
             fill_policy fp,
             const std::vector<T> &values,
             int limit)  {

    const size_type                 count = col_names.size();
    std::vector<std::future<void>>  futures(get_thread_level());
    size_type                       thread_count = 0;

    for (size_type i = 0; i < count; ++i)  {
        ColumnVecType<T>    &vec = get_column<T>(col_names[i]);

        if (fp == fill_policy::value)  {
            if (thread_count >= get_thread_level())
                fill_missing_value_(vec, values[i], limit, indices_.size());
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_value_<T>,
                               std::ref(vec),
                               std::cref(values[i]),
                               limit,
                               indices_.size());
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::fill_forward)  {
            if (thread_count >= get_thread_level())
                fill_missing_ffill_<T>(vec, limit, indices_.size());
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_ffill_<T>,
                               std::ref(vec),
                               limit,
                               indices_.size());
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::fill_backward)  {
            if (thread_count >= get_thread_level())
                fill_missing_bfill_<T>(vec, limit);
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_bfill_<T>,
                               std::ref(vec),
                               limit);
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::linear_interpolate)  {
            if (thread_count >= get_thread_level())
                fill_missing_linter_<T>(vec, indices_, limit);
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_linter_<T>,
                               std::ref(vec),
                               std::cref(indices_),
                               limit);
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::mid_point)  {
            if (thread_count >= get_thread_level())
                fill_missing_midpoint_<T>(vec, limit, indices_.size());
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_midpoint_<T>,
                               std::ref(vec),
                               limit,
                               indices_.size());
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::linear_extrapolate)  {
            char buffer [512];

            sprintf (
                buffer,
                "DataFrame::fill_missing(): fill_policy %d is not implemented",
                static_cast<int>(fp));
            throw NotImplemented(buffer);
        }
    }

    for (size_type idx = 0; idx < thread_count; ++idx)
        futures[idx].get();
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
void DataFrame<I, H>::fill_missing (const DF &rhs)  {

    const auto  &self_idx = get_index();
    const auto  &rhs_idx = rhs.get_index();

    for (const auto &col_citer : column_list_)  {
        fill_missing_functor_<DF, Ts ...>   functor (
            self_idx, rhs_idx, rhs, col_citer.first.c_str());
        const SpinGuard                     guard(lock_);

        data_[col_citer.second].change(functor);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
drop_missing_rows_(T &vec,
                   const DropRowMap missing_row_map,
                   drop_policy policy,
                   size_type threshold,
                   size_type col_num)  {

    size_type   erase_count = 0;

    for (const auto &iter : missing_row_map)  {
        if (policy == drop_policy::all)  {
            if (iter.second == col_num)  {
                vec.erase(vec.begin() + (iter.first - erase_count));
                erase_count += 1;
            }
        }
        else if (policy == drop_policy::any)  {
            if (iter.second > 0)  {
                vec.erase(vec.begin() + (iter.first - erase_count));
                erase_count += 1;
            }
        }
        else if (policy == drop_policy::threshold)  {
            if (iter.second > threshold)  {
                vec.erase(vec.begin() + (iter.first - erase_count));
                erase_count += 1;
            }
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::
drop_missing(drop_policy policy, size_type threshold)  {

    DropRowMap      missing_row_map;
    const size_type data_size = data_.size();

    {
        map_missing_rows_functor_<Ts ...>   functor (
            indices_.size(), missing_row_map);
        const SpinGuard                     guard(lock_);

        for (size_type idx = 0; idx < data_size; ++idx)
            data_[idx].change(functor);
    }

    drop_missing_rows_(indices_, missing_row_map, policy, threshold, data_size);

    drop_missing_rows_functor_<Ts ...>  functor2 (
        missing_row_map, policy, threshold, data_.size());
    const SpinGuard                     guard(lock_);

    for (size_type idx = 0; idx < data_size; ++idx)
        data_[idx].change(functor2);

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::size_type DataFrame<I, H>::
replace(const char *col_name,
        const std::vector<T> &old_values,
        const std::vector<T> &new_values,
        int limit)  {

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    size_type           count = 0;

    _replace_vector_vals_<ColumnVecType<T>, T>
        (vec, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::size_type DataFrame<I, H>::
replace_index(const std::vector<IndexType> &old_values,
              const std::vector<IndexType> &new_values,
              int limit)  {

    size_type   count = 0;

    _replace_vector_vals_<IndexVecType, IndexType>
        (indices_, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename F>
void DataFrame<I, H>::
replace(const char *col_name, F &functor)  {

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    const size_type     vec_s = vec.size();

    for (size_type i = 0; i < vec_s; ++i)
        if (! functor(indices_[i], vec[i]))  break;

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
std::future<typename DataFrame<I, H>::size_type> DataFrame<I, H>::
replace_async(const char *col_name,
              const std::vector<T> &old_values,
              const std::vector<T> &new_values,
              int limit)  {

    return (std::async(std::launch::async,
                       &DataFrame::replace<T>,
                           this,
                           col_name,
                           std::forward<const std::vector<T>>(old_values),
                           std::forward<const std::vector<T>>(new_values),
                           limit));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename F>
std::future<void> DataFrame<I, H>::
replace_async(const char *col_name, F &functor)  {

    return (std::async(std::launch::async,
                       &DataFrame::replace<T, F>,
                           this,
                           col_name,
                           std::ref(functor)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ...Ts>
void DataFrame<I, H>::make_consistent ()  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call make_consistent()");

    const size_type             idx_s = indices_.size();
    consistent_functor_<Ts ...> functor (idx_s);
    const SpinGuard             guard(lock_);

    for (const auto &iter : data_)
        iter.change(functor);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ...Ts>
void DataFrame<I, H>::shrink_to_fit ()  {

    indices_.shrink_to_fit();

    shrink_to_fit_functor_<Ts ...>  functor;

    for (const auto &iter : data_)  {
        const SpinGuard guard(lock_);

        iter.change(functor);
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ...Ts>
void DataFrame<I, H>::sort(const char *name, sort_spec dir)  {

    make_consistent<Ts ...>();

    if (! ::strcmp(name, DF_INDEX_COL_NAME))  {
        auto    a = [this](size_type i, size_type j) -> bool  {
                        return (this->indices_[i] < this->indices_[j]);
                    };
        auto    d = [this](size_type i, size_type j) -> bool  {
                        return (this->indices_[i] > this->indices_[j]);
                    };


        if (dir == sort_spec::ascen)
            sort_common_<decltype(a), Ts ...>(*this, std::move(a));
        else
            sort_common_<decltype(d), Ts ...>(*this, std::move(d));
    }
    else  {
        const ColumnVecType<T>  &idx_vec = get_column<T>(name);

        auto    a = [&x = idx_vec](size_type i, size_type j) -> bool {
                        return (x[i] < x[j]);
                    };
        auto    d = [&x = idx_vec](size_type i, size_type j) -> bool {
                        return (x[i] > x[j]);
                    };

        if (dir == sort_spec::ascen)
            sort_common_<decltype(a), Ts ...>(*this, std::move(a));
        else
            sort_common_<decltype(d), Ts ...>(*this, std::move(d));
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1, const char *name2, sort_spec dir2)  {

    make_consistent<Ts ...>();

    ColumnVecType<T1>   *vec1 { nullptr};
    ColumnVecType<T2>   *vec2 { nullptr};

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    auto    cf =
        [vec1, vec2, dir1, dir2](size_type i, size_type j) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (vec1->at(i) < vec1->at(j))
                    return (true);
                else if (vec1->at(i) > vec1->at(j))
                    return (false);
            }
            else  {
                if (vec1->at(i) > vec1->at(j))
                    return (true);
                else if (vec1->at(i) < vec1->at(j))
                    return (false);
            }
            if (dir2 == sort_spec::ascen)
                return (vec2->at(i) < vec2->at(j));
            else
                return (vec2->at(i) > vec2->at(j));
        };

    sort_common_<decltype(cf), Ts ...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     const char *name3, sort_spec dir3)  {

    make_consistent<Ts ...>();

    ColumnVecType<T1>   *vec1 { nullptr};
    ColumnVecType<T2>   *vec2 { nullptr};
    ColumnVecType<T3>   *vec3 { nullptr};

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    if (! ::strcmp(name3, DF_INDEX_COL_NAME))
        vec3 = reinterpret_cast<ColumnVecType<T3> *>(&indices_);
    else
        vec3 = &(get_column<T3>(name3));

    auto    cf =
        [vec1, vec2, vec3, dir1, dir2, dir3]
        (size_type i, size_type j) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (vec1->at(i) < vec1->at(j))
                    return (true);
                else if (vec1->at(i) > vec1->at(j))
                    return (false);
            }
            else  {
                if (vec1->at(i) > vec1->at(j))
                    return (true);
                else if (vec1->at(i) < vec1->at(j))
                    return (false);
            }
            if (dir2 == sort_spec::ascen)  {
                if (vec2->at(i) < vec2->at(j))
                    return (true);
                else if (vec2->at(i) > vec2->at(j))
                    return (false);
            }
            else  {
                if (vec2->at(i) > vec2->at(j))
                    return (true);
                else if (vec2->at(i) < vec2->at(j))
                    return (false);
            }
            if (dir3 == sort_spec::ascen)
                return (vec3->at(i) < vec3->at(j));
            else
                return (vec3->at(i) > vec3->at(j));
        };

    sort_common_<decltype(cf), Ts ...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     const char *name3, sort_spec dir3,
     const char *name4, sort_spec dir4)  {

    make_consistent<Ts ...>();

    ColumnVecType<T1>   *vec1 { nullptr};
    ColumnVecType<T2>   *vec2 { nullptr};
    ColumnVecType<T3>   *vec3 { nullptr};
    ColumnVecType<T4>   *vec4 { nullptr};

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    if (! ::strcmp(name3, DF_INDEX_COL_NAME))
        vec3 = reinterpret_cast<ColumnVecType<T3> *>(&indices_);
    else
        vec3 = &(get_column<T3>(name3));

    if (! ::strcmp(name4, DF_INDEX_COL_NAME))
        vec4 = reinterpret_cast<ColumnVecType<T4> *>(&indices_);
    else
        vec4 = &(get_column<T4>(name4));

    auto    cf =
        [vec1, vec2, vec3, vec4, dir1, dir2, dir3, dir4]
        (size_type i, size_type j) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (vec1->at(i) < vec1->at(j))
                    return (true);
                else if (vec1->at(i) > vec1->at(j))
                    return (false);
            }
            else  {
                if (vec1->at(i) > vec1->at(j))
                    return (true);
                else if (vec1->at(i) < vec1->at(j))
                    return (false);
            }
            if (dir2 == sort_spec::ascen)  {
                if (vec2->at(i) < vec2->at(j))
                    return (true);
                else if (vec2->at(i) > vec2->at(j))
                    return (false);
            }
            else  {
                if (vec2->at(i) > vec2->at(j))
                    return (true);
                else if (vec2->at(i) < vec2->at(j))
                    return (false);
            }
            if (dir3 == sort_spec::ascen)  {
                if (vec3->at(i) < vec3->at(j))
                    return (true);
                else if (vec3->at(i) > vec3->at(j))
                    return (false);
            }
            else  {
                if (vec3->at(i) > vec3->at(j))
                    return (true);
                else if (vec3->at(i) < vec3->at(j))
                    return (false);
            }
            if (dir4 == sort_spec::ascen)
                return (vec4->at(i) < vec4->at(j));
            else
                return (vec4->at(i) > vec4->at(j));
        };

    sort_common_<decltype(cf), Ts ...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     const char *name3, sort_spec dir3,
     const char *name4, sort_spec dir4,
     const char *name5, sort_spec dir5)  {

    make_consistent<Ts ...>();

    ColumnVecType<T1>   *vec1 { nullptr};
    ColumnVecType<T2>   *vec2 { nullptr};
    ColumnVecType<T3>   *vec3 { nullptr};
    ColumnVecType<T4>   *vec4 { nullptr};
    ColumnVecType<T5>   *vec5 { nullptr};

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    if (! ::strcmp(name3, DF_INDEX_COL_NAME))
        vec3 = reinterpret_cast<ColumnVecType<T3> *>(&indices_);
    else
        vec3 = &(get_column<T3>(name3));

    if (! ::strcmp(name4, DF_INDEX_COL_NAME))
        vec4 = reinterpret_cast<ColumnVecType<T4> *>(&indices_);
    else
        vec4 = &(get_column<T4>(name4));

    if (! ::strcmp(name4, DF_INDEX_COL_NAME))
        vec5 = reinterpret_cast<ColumnVecType<T5> *>(&indices_);
    else
        vec5 = &(get_column<T5>(name5));

    auto    cf =
        [vec1, vec2, vec3, vec4, vec5, dir1, dir2, dir3, dir4, dir5]
        (size_type i, size_type j) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (vec1->at(i) < vec1->at(j))
                    return (true);
                else if (vec1->at(i) > vec1->at(j))
                    return (false);
            }
            else  {
                if (vec1->at(i) > vec1->at(j))
                    return (true);
                else if (vec1->at(i) < vec1->at(j))
                    return (false);
            }
            if (dir2 == sort_spec::ascen)  {
                if (vec2->at(i) < vec2->at(j))
                    return (true);
                else if (vec2->at(i) > vec2->at(j))
                    return (false);
            }
            else  {
                if (vec2->at(i) > vec2->at(j))
                    return (true);
                else if (vec2->at(i) < vec2->at(j))
                    return (false);
            }
            if (dir3 == sort_spec::ascen)  {
                if (vec3->at(i) < vec3->at(j))
                    return (true);
                else if (vec3->at(i) > vec3->at(j))
                    return (false);
            }
            else  {
                if (vec3->at(i) > vec3->at(j))
                    return (true);
                else if (vec3->at(i) < vec3->at(j))
                    return (false);
            }
            if (dir4 == sort_spec::ascen)  {
                if (vec4->at(i) < vec4->at(j))
                    return (true);
                else if (vec4->at(i) > vec4->at(j))
                    return (false);
            }
            else  {
                if (vec4->at(i) > vec4->at(j))
                    return (true);
                else if (vec4->at(i) < vec4->at(j))
                    return (false);
            }
            if (dir5 == sort_spec::ascen)
                return (vec5->at(i) < vec5->at(j));
            else
                return (vec5->at(i) > vec5->at(j));
        };

    sort_common_<decltype(cf), Ts ...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ...Ts>
std::future<void>
DataFrame<I, H>::sort_async(const char *name, sort_spec dir)  {

    return (std::async(std::launch::async,
                       [name, dir, this] () -> void {
                           this->sort<T, Ts ...>(name, dir);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2)  {

    return (std::async(std::launch::async,
                       [name1, dir1, name2, dir2, this] () -> void {
                           this->sort<T1, T2, Ts ...>(name1, dir1,
                                                      name2, dir2);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           const char *name3, sort_spec dir3)  {

    return (std::async(std::launch::async,
                       [name1, dir1, name2, dir2, name3, dir3,
                        this] () -> void {
                           this->sort<T1, T2, T3, Ts ...>(name1, dir1,
                                                          name2, dir2,
                                                          name3, dir3);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           const char *name3, sort_spec dir3,
           const char *name4, sort_spec dir4)  {

    return (std::async(std::launch::async,
                       [name1, dir1, name2, dir2, name3, dir3, name4, dir4,
                        this] () -> void {
                           this->sort<T1, T2, T3, T4, Ts ...>(name1, dir1,
                                                              name2, dir2,
                                                              name3, dir3,
                                                              name4, dir4);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           const char *name3, sort_spec dir3,
           const char *name4, sort_spec dir4,
           const char *name5, sort_spec dir5)  {

    return (std::async(std::launch::async,
                       [name1, dir1, name2, dir2, name3, dir3, name4, dir4,
                        name5, dir5, this] () -> void {
                           this->sort<T1, T2, T3, T4, T5, Ts ...>(name1, dir1,
                                                                  name2, dir2,
                                                                  name3, dir3,
                                                                  name4, dir4,
                                                                  name5, dir5);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename I_V, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
groupby1(const char *col_name, I_V &&idx_visitor, Ts&& ... args) const  {
    using SorterKey = T;
    using SorterVal = std::tuple<IndexType, typename std::tuple_element<2, Ts>::type::value_type...>;
    auto sharded_sorter = nu::make_sharded_sorter<SorterKey, SorterVal>();

    auto &index       = const_cast<NuShardedVector<IndexType>&>(get_index());
    auto &key         = const_cast<NuShardedVector<T>&>(get_column<T>(col_name));
    auto agg_cols     = std::forward_as_tuple(
        const_cast<NuShardedVector<typename std::tuple_element<2, Ts>::type::value_type>&>(
            get_column<typename std::tuple_element<2, Ts>::type::value_type>(
                std::get<0>(args)))...);
    auto sealed_index    = nu::to_sealed_ds(std::move(index));
    auto sealed_key      = nu::to_sealed_ds(std::move(key));
    auto sealed_agg_cols = std::apply(
        [&](auto&... col) { return std::make_tuple(nu::to_sealed_ds(std::move(col))...); },
        agg_cols);
    auto index_iter    = sealed_index.cbegin();
    auto key_iter      = sealed_key.cbegin();
    auto agg_col_iters = std::apply(
        [&](auto&... sealed_col) { return std::make_tuple(sealed_col.cbegin()...); },
        sealed_agg_cols);

    while (index_iter != sealed_index.cend()) {
        auto idx = *index_iter;
        auto k   = *key_iter;
        auto v   = std::apply([&](auto&... iter) { return std::make_tuple(idx, (*iter)...); },
                            agg_col_iters);
        sharded_sorter.emplace(std::move(k), std::move(v));
        ++index_iter;
        ++key_iter;
        std::apply([&](auto&... iter) { ((++iter), ...); }, agg_col_iters);
    }

    DataFrame res;
    auto &dst_idx = res.get_index();
    auto *dst_key = ::strcmp(col_name, DF_INDEX_COL_NAME)
                        ? &(res.template create_column<T>(col_name))
                        : nullptr;
    auto dst_cols = std::make_tuple(
        (&(res.template create_column<typename std::tuple_element<2, Ts>::type::value_type>(
            std::get<1>(args))))...);
    auto sharded_sorted           = sharded_sorter.sort();
    T marker                      = sharded_sorted.cbegin()->first;

    idx_visitor.pre();
    ((std::get<2>(args).pre()), ...);
    for (const auto &[k, v] : sharded_sorted) {
        auto idx    = std::get<0>(v);
        if (k != marker) {
            idx_visitor.post();
            ((std::get<2>(args).post()), ...);
            dst_idx.push_back(idx_visitor.get_result());
            std::apply(
                [&](auto&... col) { ((col->push_back(std::get<2>(args).get_result())), ...); },
                dst_cols);
            if (dst_key) dst_key->push_back(marker);
            marker = k;
            idx_visitor.pre();
            ((std::get<2>(args).pre()), ...);
        }
        idx_visitor(idx, idx);
        std::apply([&](auto idx, auto&... col_vals) { ((std::get<2>(args)(idx, col_vals)), ...); },
                   v);
    }
    if (sharded_sorted.size()) {
        idx_visitor.post();
        ((std::get<2>(args).post()), ...);
        dst_idx.push_back(idx_visitor.get_result());
        std::apply([&](auto&... col) { ((col->push_back(std::get<2>(args).get_result())), ...); },
                   dst_cols);
        if (dst_key) dst_key->push_back(marker);
    }

    index = nu::to_unsealed_ds(std::move(sealed_index));
    std::apply(
        [&](auto&... sealed_cols) {
            std::apply(
                [&](auto&... cols) { ((cols = nu::to_unsealed_ds(std::move(sealed_cols))), ...); },
                agg_cols);
        },
        sealed_agg_cols);

    return (res);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename I_V, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
groupby2(const char *col_name1,
         const char *col_name2,
         I_V &&idx_visitor,
         Ts&& ... args) const  {

    const ColumnVecType<T1> *gb_vec1 { nullptr };
    const ColumnVecType<T2> *gb_vec2 { nullptr };

    if (! ::strcmp(col_name1, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_index());
        gb_vec2 = (const ColumnVecType<T2> *) &(get_column<T2>(col_name2));
    }
    else if (! ::strcmp(col_name2, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_column<T1>(col_name1));
        gb_vec2 = (const ColumnVecType<T2> *) &(get_index());
    }
    else  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_column<T1>(col_name1));
        gb_vec2 = (const ColumnVecType<T2> *) &(get_column<T2>(col_name2));
    }

    std::vector<std::size_t>    sort_v (std::min(gb_vec1->size(),
                                                 gb_vec2->size()),
                                        0);

    std::iota(sort_v.begin(), sort_v.end(), 0);
    std::sort(sort_v.begin(), sort_v.end(),
              [gb_vec1, gb_vec2](std::size_t i, std::size_t j) -> bool  {
                  if (gb_vec1->at(i) < gb_vec1->at(j))
                      return (true);
                  else if (gb_vec1->at(i) > gb_vec1->at(j))
                      return (false);
                  return (gb_vec2->at(i) < gb_vec2->at(j));
              });

    DataFrame   res;
    auto        args_tuple = std::tuple<Ts ...>(args ...);
    auto        func =
        [*this,
         &res,
         gb_vec1,
         gb_vec2,
         &sort_v,
         idx_visitor = std::forward<I_V>(idx_visitor),
         col_name1,
         col_name2](auto &triple) mutable -> void {
            _load_groupby_data_2_(*this,
                                  res,
                                  triple,
                                  idx_visitor,
                                  *gb_vec1,
                                  *gb_vec2,
                                  sort_v,
                                  col_name1,
                                  col_name2);
        };

    for_each_in_tuple (args_tuple, func);

    return (res);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename I_V, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
groupby3(const char *col_name1,
         const char *col_name2,
         const char *col_name3,
         I_V &&idx_visitor,
         Ts&& ... args) const  {

    const ColumnVecType<T1> *gb_vec1 { nullptr };
    const ColumnVecType<T2> *gb_vec2 { nullptr };
    const ColumnVecType<T3> *gb_vec3 { nullptr };

    if (! ::strcmp(col_name1, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_index());
        gb_vec2 = (const ColumnVecType<T2> *) &(get_column<T2>(col_name2));
        gb_vec3 = (const ColumnVecType<T3> *) &(get_column<T3>(col_name3));
    }
    else if (! ::strcmp(col_name2, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_column<T1>(col_name1));
        gb_vec2 = (const ColumnVecType<T2> *) &(get_index());
        gb_vec3 = (const ColumnVecType<T3> *) &(get_column<T3>(col_name3));
    }
    else if (! ::strcmp(col_name3, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_column<T1>(col_name1));
        gb_vec2 = (const ColumnVecType<T2> *) &(get_column<T2>(col_name2));
        gb_vec3 = (const ColumnVecType<T3> *) &(get_index());
    }
    else  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_column<T1>(col_name1));
        gb_vec2 = (const ColumnVecType<T2> *) &(get_column<T2>(col_name2));
        gb_vec3 = (const ColumnVecType<T3> *) &(get_column<T3>(col_name3));
    }

    std::vector<std::size_t>    sort_v(
        std::min({ gb_vec1->size(), gb_vec2->size(), gb_vec3->size() }), 0);

    std::iota(sort_v.begin(), sort_v.end(), 0);
    std::sort(sort_v.begin(), sort_v.end(),
              [gb_vec1, gb_vec2, gb_vec3](std::size_t i,
                                          std::size_t j) -> bool  {
                  if (gb_vec1->at(i) < gb_vec1->at(j))
                      return (true);
                  else if (gb_vec1->at(i) > gb_vec1->at(j))
                      return (false);
                  else if (gb_vec2->at(i) < gb_vec2->at(j))
                      return (true);
                  else if (gb_vec2->at(i) > gb_vec2->at(j))
                      return (false);
                  return (gb_vec3->at(i) < gb_vec3->at(j));
              });

    DataFrame   res;
    auto        args_tuple = std::tuple<Ts ...>(args ...);
    auto        func =
        [*this,
         &res,
         gb_vec1,
         gb_vec2,
         gb_vec3,
         &sort_v,
         idx_visitor = std::forward<I_V>(idx_visitor),
         col_name1,
         col_name2,
         col_name3](auto &triple) mutable -> void {
            _load_groupby_data_3_(*this,
                                  res,
                                  triple,
                                  idx_visitor,
                                  *gb_vec1,
                                  *gb_vec2,
                                  *gb_vec3,
                                  sort_v,
                                  col_name1,
                                  col_name2,
                                  col_name3);
        };

    for_each_in_tuple (args_tuple, func);

    return (res);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename I_V, typename ... Ts>
std::future<DataFrame<I, H>> DataFrame<I, H>::
groupby1_async(const char *col_name, I_V &&idx_visitor, Ts&& ... args) const {

    return (std::async(std::launch::async,
                       &DataFrame::groupby1<T, I_V, Ts ...>,
                           this,
                           col_name,
                           std::forward<I_V>(idx_visitor),
                           std::forward<Ts>(args) ...));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename I_V, typename ... Ts>
std::future<DataFrame<I, H>> DataFrame<I, H>::
groupby2_async(const char *col_name1,
               const char *col_name2,
               I_V &&idx_visitor,
               Ts&& ... args) const  {

    return (std::async(std::launch::async,
                       &DataFrame::groupby2<T1, T2, I_V, Ts ...>,
                           this,
                           col_name1,
                           col_name2,
                           std::forward<I_V>(idx_visitor),
                           std::forward<Ts>(args) ...));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename I_V, typename ... Ts>
std::future<DataFrame<I, H>> DataFrame<I, H>::
groupby3_async(const char *col_name1,
               const char *col_name2,
               const char *col_name3,
               I_V &&idx_visitor,
               Ts&& ... args) const  {

    return (std::async(std::launch::async,
                       &DataFrame::groupby3<T1, T2, T3, I_V, Ts ...>,
                           this,
                           col_name1,
                           col_name2,
                           col_name3,
                           std::forward<I_V>(idx_visitor),
                           std::forward<Ts>(args) ...));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
StdDataFrame<T>
DataFrame<I, H>::value_counts (const char *col_name) const  {

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    auto                    hash_func =
        [](std::reference_wrapper<const T> v) -> std::size_t  {
            return(std::hash<T>{}(v.get()));
    };
    auto                    equal_func =
        [](std::reference_wrapper<const T> lhs,
           std::reference_wrapper<const T> rhs) -> bool  {
            return(lhs.get() == rhs.get());
    };

    std::unordered_map<
        typename std::reference_wrapper<const T>::type,
        size_type,
        decltype(hash_func),
        decltype(equal_func)>   values_map(vec.size(), hash_func, equal_func);
    size_type                   nan_count = 0;

    // take care of nans
    for (const auto &citer : vec)  {
        if (is_nan<T>(citer))  {
            ++nan_count;
            continue;
        }

        auto    insert_result = values_map.emplace(std::ref(citer), 1);

        if (insert_result.second == false)
            insert_result.first->second += 1;
    }

    std::vector<T>          res_indices;
    std::vector<size_type>  counts;

    counts.reserve(values_map.size());
    res_indices.reserve(values_map.size());

    for (const auto &citer : values_map)  {
        res_indices.push_back(citer.first);
        counts.emplace_back(citer.second);
    }
    if (nan_count > 0)  {
        res_indices.push_back(get_nan<T>());
        counts.emplace_back(nan_count);
    }

    StdDataFrame<T> result_df;

    result_df.load_index(std::move(res_indices));
    result_df.load_column("counts", std::move(counts));

    return(result_df);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
StdDataFrame<T> DataFrame<I, H>::value_counts(size_type index) const  {

    return (value_counts<T>(column_list_[index].first.c_str()));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V, typename I_V, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
bucketize(bucket_type bt,
          const V &value,
          I_V &&idx_visitor,
          Ts&& ... args) const  {

    DataFrame       result;
    auto            &dst_idx = result.get_index();
    const auto      &src_idx = get_index();
    const size_type idx_s = src_idx.size();

    _bucketize_core_(dst_idx, src_idx, src_idx, value, idx_visitor, idx_s, bt);

    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    func =
        [this, &result, &value, bt](auto &triple) mutable -> void {
            _load_bucket_data_(*this, result, value, bt, triple);
        };

    for_each_in_tuple (args_tuple, func);

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V, typename I_V, typename ... Ts>
std::future<DataFrame<I, H>>
DataFrame<I, H>::
bucketize_async(bucket_type bt,
                const V &value,
                I_V &&idx_visitor,
                Ts&& ... args) const  {

    return (std::async(std::launch::async,
                       &DataFrame::bucketize<V, I_V, Ts ...>,
                           this,
                           bt,
                           std::cref(value),
                           std::forward<I_V>(idx_visitor),
                           std::forward<Ts>(args) ...));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename V>
DataFrame<I, H> DataFrame<I, H>::
transpose(IndexVecType &&indices, const V &new_col_names) const  {

    const size_type num_cols = column_list_.size();

    if (new_col_names.size() != indices_.size())
        throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                "Length of new_col_names is not equal "
                                "to number of rows");

    std::vector<const std::vector<T> *> current_cols;

    current_cols.reserve(num_cols);
    for (const auto &citer : column_list_)
        current_cols.push_back(&(get_column<T>(citer.first.c_str())));

    std::vector<std::vector<T>> trans_cols(indices_.size());
    DataFrame                   df;

    for (size_type i = 0; i < indices_.size(); ++i)  {
        trans_cols[i].reserve(num_cols);
        for (size_type j = 0; j < num_cols; ++j)  {
            if (current_cols[j]->size() > i)
                trans_cols[i].push_back((*(current_cols[j]))[i]);
            else
                trans_cols[i].push_back(get_nan<T>());
        }
    }

    df.load_index(std::move(indices));
    for (size_type i = 0; i < new_col_names.size(); ++i)
        df.load_column(&(new_col_names[i][0]), std::move(trans_cols[i]));

    return (df);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
