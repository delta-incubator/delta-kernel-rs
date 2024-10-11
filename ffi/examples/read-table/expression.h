#include "delta_kernel_ffi.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/**
 * This module defines a very simple model of an expression, used only to be able to print the
 * provided expression. It consists of an "ExpressionBuilder" which is our user data that gets
 * passed into each visit_x call. This simply keeps track of all the lists we are asked to allocate.
 *
 * Each item "ExpressionItem", which tracks the type and pointer to the expression.
 *
 * Each complex type is made of an "ExpressionItemList", which tracks its  length and an array of
 * "ExpressionItems". The top level expression is in a length 1 "ExpressionItemList".
 */

/*************************************************************
 * Data Types
 ************************************************************/

enum OpType
{
  Add,
  Minus,
  Divide,
  Multiply,
  LessThan,
  LessThanOrEqual,
  GreaterThan,
  GreaterThaneOrEqual,
  Equal,
  NotEqual,
  Distinct,
  In,
  NotIn,
};
enum LitType
{
  Integer,
  Long,
  Short,
  Byte,
  Float,
  Double,
  String,
  Boolean,
  Timestamp,
  TimestampNtz,
  Date,
  Binary,
  Decimal,
  Null,
  Struct,
  Array
};
enum ExpressionType
{
  BinOp,
  Variadic,
  Literal,
  Unary,
  Column
};
typedef struct
{
  void* ref;
  enum ExpressionType type;
} ExpressionItem;

typedef struct
{
  uint32_t len;
  ExpressionItem* exprList;
} ExpressionItemList;
struct BinOp
{
  enum OpType op;
  ExpressionItemList exprs;
};
struct Null;

enum VariadicType
{
  And,
  Or,
  StructExpression,
  ArrayData
};
enum UnaryType
{
  Not,
  IsNull
};
struct Variadic
{
  enum VariadicType op;
  ExpressionItemList expr_list;
};
struct Unary
{
  enum UnaryType type;
  ExpressionItemList sub_expr;
};
struct BinaryData
{
  uint8_t* buf;
  uintptr_t len;
};
struct Decimal
{
  uint64_t value[2];
  uint8_t precision;
  uint8_t scale;
};
typedef struct
{
  size_t list_count;
  ExpressionItemList* lists;
} ExpressionBuilder;
struct Struct
{
  ExpressionItemList fields;
  ExpressionItemList values;
};

struct ArrayData
{
  ExpressionItemList expr_list;
};

struct Literal
{
  enum LitType type;
  union LiteralValue
  {
    int32_t integer_data;
    int64_t long_data;
    int16_t short_data;
    int8_t byte_data;
    float float_data;
    double double_data;
    bool boolean_data;
    char* string_data;
    struct Struct struct_data;
    struct ArrayData array_data;
    struct BinaryData binary;
    struct Decimal decimal;
  } value;
};

/*************************************************************
 * Utilitiy functions
 ************************************************************/

void put_handle(void* data, void* ref, enum ExpressionType type, size_t sibling_list_id)
{
  ExpressionBuilder* data_ptr = (ExpressionBuilder*)data;
  ExpressionItem expr = { .ref = ref, .type = type };
  ExpressionItemList* list = &data_ptr->lists[sibling_list_id];
  list->exprList[list->len++] = expr;
}
ExpressionItemList get_handle(void* data, size_t list_id)
{
  ExpressionBuilder* data_ptr = (ExpressionBuilder*)data;
  if (list_id > data_ptr->list_count) {
    abort();
  }
  return data_ptr->lists[list_id];
}

// utility to turn a slice into a char*
char* allocate_string(const KernelStringSlice slice)
{
  return strndup(slice.ptr, slice.len);
}

/*************************************************************
 * Binary Operations
 ************************************************************/

#define DEFINE_BINOP(fun_name, op)                                                                 \
  void fun_name(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)                    \
  {                                                                                                \
    visit_expr_binop(data, op, child_list_id, sibling_list_id);                                    \
  }
void visit_expr_binop(
  void* data,
  enum OpType op,
  uintptr_t child_id_list,
  uintptr_t sibling_id_list)
{
  struct BinOp* binop = malloc(sizeof(struct BinOp));
  binop->op = op;
  binop->exprs = get_handle(data, child_id_list);
  put_handle(data, binop, BinOp, sibling_id_list);
}
DEFINE_BINOP(visit_expr_add, Add)
DEFINE_BINOP(visit_expr_minus, Minus)
DEFINE_BINOP(visit_expr_multiply, Multiply)
DEFINE_BINOP(visit_expr_divide, Divide)
DEFINE_BINOP(visit_expr_lt, LessThan)
DEFINE_BINOP(visit_expr_le, LessThanOrEqual)
DEFINE_BINOP(visit_expr_gt, GreaterThan)
DEFINE_BINOP(visit_expr_ge, GreaterThaneOrEqual)
DEFINE_BINOP(visit_expr_eq, Equal)
DEFINE_BINOP(visit_expr_ne, NotEqual)
DEFINE_BINOP(visit_expr_distinct, Distinct)
DEFINE_BINOP(visit_expr_in, In)
DEFINE_BINOP(visit_expr_not_in, NotIn)

/*************************************************************
 * Literal Values
 ************************************************************/

#define DEFINE_SIMPLE_SCALAR(fun_name, enum_member, c_type, literal_field)                         \
  void fun_name(void* data, c_type val, uintptr_t sibling_list_id)                                 \
  {                                                                                                \
    struct Literal* lit = malloc(sizeof(struct Literal));                                          \
    lit->type = enum_member;                                                                       \
    lit->value.literal_field = val;                                                                \
    put_handle(data, lit, Literal, sibling_list_id);                                               \
  }                                                                                                \
  _Static_assert(                                                                                  \
    sizeof(c_type) <= sizeof(uintptr_t), "The provided type is not a valid simple scalar")
DEFINE_SIMPLE_SCALAR(visit_expr_int_literal, Integer, int32_t, integer_data);
DEFINE_SIMPLE_SCALAR(visit_expr_long_literal, Long, int64_t, long_data);
DEFINE_SIMPLE_SCALAR(visit_expr_short_literal, Short, int16_t, short_data);
DEFINE_SIMPLE_SCALAR(visit_expr_byte_literal, Byte, int8_t, byte_data);
DEFINE_SIMPLE_SCALAR(visit_expr_float_literal, Float, float, float_data);
DEFINE_SIMPLE_SCALAR(visit_expr_double_literal, Double, double, double_data);
DEFINE_SIMPLE_SCALAR(visit_expr_boolean_literal, Boolean, _Bool, boolean_data);
DEFINE_SIMPLE_SCALAR(visit_expr_timestamp_literal, Timestamp, int64_t, long_data);
DEFINE_SIMPLE_SCALAR(visit_expr_timestamp_ntz_literal, TimestampNtz, int64_t, long_data);
DEFINE_SIMPLE_SCALAR(visit_expr_date_literal, Date, int32_t, integer_data);

void visit_expr_string_literal(void* data, KernelStringSlice string, uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = String;
  literal->value.string_data = allocate_string(string);
  put_handle(data, literal, Literal, sibling_list_id);
}

void visit_expr_decimal_literal(
  void* data,
  uint64_t value_ms,
  uint64_t value_ls,
  uint8_t precision,
  uint8_t scale,
  uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Decimal;
  struct Decimal* dec = &literal->value.decimal;
  dec->value[0] = value_ms;
  dec->value[1] = value_ls;
  dec->precision = precision;
  dec->scale = scale;
  put_handle(data, literal, Literal, sibling_list_id);
}

void visit_expr_binary_literal(
  void* data,
  const uint8_t* buf,
  uintptr_t len,
  uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Binary;
  struct BinaryData* bin = &literal->value.binary;
  bin->buf = malloc(len);
  memcpy(bin->buf, buf, len);
  put_handle(data, literal, Literal, sibling_list_id);
}

void visit_expr_struct_literal(
  void* data,
  uintptr_t child_field_list_id,
  uintptr_t child_value_list_id,
  uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Struct;
  struct Struct* struct_data = &literal->value.struct_data;
  struct_data->fields = get_handle(data, child_field_list_id);
  struct_data->values = get_handle(data, child_value_list_id);
  put_handle(data, literal, Literal, sibling_list_id);
}

void visit_expr_null_literal(void* data, uintptr_t sibling_id_list)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Null;
  put_handle(data, literal, Literal, sibling_id_list);
}

/*************************************************************
 * Variadic Expressions
 ************************************************************/

#define DEFINE_VARIADIC(fun_name, enum_member)                                                     \
  void fun_name(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)                    \
  {                                                                                                \
    visit_expr_variadic(data, enum_member, child_list_id, sibling_list_id);                        \
  }

void visit_expr_variadic(
  void* data,
  enum VariadicType op,
  uintptr_t child_list_id,
  uintptr_t sibling_list_id)
{
  struct Variadic* var = malloc(sizeof(struct Variadic));
  var->op = op;
  var->expr_list = get_handle(data, child_list_id);
  put_handle(data, var, Variadic, sibling_list_id);
}
DEFINE_VARIADIC(visit_expr_and, And)
DEFINE_VARIADIC(visit_expr_or, Or)
DEFINE_VARIADIC(visit_expr_struct_expr, StructExpression)

void visit_expr_array_literal(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Array;
  struct ArrayData* arr = &(literal->value.array_data);
  arr->expr_list = get_handle(data, child_list_id);
  put_handle(data, literal, Literal, sibling_list_id);
}

/*************************************************************
 * Unary Expressions
 ************************************************************/
#define DEFINE_UNARY(fun_name, op)                                                                 \
  void fun_name(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)                    \
  {                                                                                                \
    visit_expr_unary(data, op, child_list_id, sibling_list_id);                                    \
  }

void visit_expr_unary(
  void* data,
  enum UnaryType type,
  uintptr_t child_list_id,
  uintptr_t sibling_list_id)
{
  struct Unary* unary = malloc(sizeof(struct Unary));
  unary->type = type;
  unary->sub_expr = get_handle(data, child_list_id);
  put_handle(data, unary, Unary, sibling_list_id);
}
DEFINE_UNARY(visit_expr_is_null, IsNull)
DEFINE_UNARY(visit_expr_not, Not)

/*************************************************************
 * Column Expression
 ************************************************************/

void visit_expr_column(void* data, KernelStringSlice string, uintptr_t sibling_id_list)
{
  char* column_name = allocate_string(string);
  put_handle(data, column_name, Column, sibling_id_list);
}

uintptr_t make_field_list(void* data, uintptr_t reserve)
{
  ExpressionBuilder* builder = data;
  int id = builder->list_count;
  builder->list_count++;
  builder->lists = realloc(builder->lists, sizeof(ExpressionItemList) * builder->list_count);
  ExpressionItem* list = calloc(reserve, sizeof(ExpressionItem));
  builder->lists[id].len = 0;
  builder->lists[id].exprList = list;
  return id;
}

ExpressionItemList construct_predicate(SharedExpression* predicate)
{
  ExpressionBuilder data = { 0 };
  EngineExpressionVisitor visitor = {
    .data = &data,
    .make_field_list = make_field_list,
    .visit_int_literal = visit_expr_int_literal,
    .visit_long_literal = visit_expr_long_literal,
    .visit_short_literal = visit_expr_short_literal,
    .visit_byte_literal = visit_expr_byte_literal,
    .visit_float_literal = visit_expr_float_literal,
    .visit_double_literal = visit_expr_double_literal,
    .visit_bool_literal = visit_expr_boolean_literal,
    .visit_timestamp_literal = visit_expr_timestamp_literal,
    .visit_timestamp_ntz_literal = visit_expr_timestamp_ntz_literal,
    .visit_date_literal = visit_expr_date_literal,
    .visit_binary_literal = visit_expr_binary_literal,
    .visit_null_literal = visit_expr_null_literal,
    .visit_decimal_literal = visit_expr_decimal_literal,
    .visit_string_literal = visit_expr_string_literal,
    .visit_struct_literal = visit_expr_struct_literal,
    .visit_array_literal = visit_expr_array_literal,
    .visit_and = visit_expr_and,
    .visit_or = visit_expr_or,
    .visit_not = visit_expr_not,
    .visit_is_null = visit_expr_is_null,
    .visit_lt = visit_expr_lt,
    .visit_le = visit_expr_le,
    .visit_gt = visit_expr_gt,
    .visit_ge = visit_expr_ge,
    .visit_eq = visit_expr_eq,
    .visit_ne = visit_expr_ne,
    .visit_distinct = visit_expr_distinct,
    .visit_in = visit_expr_in,
    .visit_not_in = visit_expr_not_in,
    .visit_add = visit_expr_add,
    .visit_minus = visit_expr_minus,
    .visit_multiply = visit_expr_multiply,
    .visit_divide = visit_expr_divide,
    .visit_column = visit_expr_column,
    .visit_struct_expr = visit_expr_struct_expr,
  };
  uintptr_t top_level_id = visit_expression(&predicate, &visitor);
  ExpressionItemList top_level_expr = data.lists[top_level_id];
  free(data.lists);
  return top_level_expr;
}

void free_expression_item_list(ExpressionItemList list);

void free_expression_item(ExpressionItem ref)
{
  switch (ref.type) {
    case BinOp: {
      struct BinOp* op = ref.ref;
      free_expression_item_list(op->exprs);
      free(op);
      break;
    }
    case Variadic: {
      struct Variadic* var = ref.ref;
      free_expression_item_list(var->expr_list);
      free(var);
      break;
    };
    case Literal: {
      struct Literal* lit = ref.ref;
      switch (lit->type) {
        case Struct: {
          struct Struct* struct_data = &lit->value.struct_data;
          free_expression_item_list(struct_data->values);
          free_expression_item_list(struct_data->fields);
          break;
        }
        case Array: {
          struct ArrayData* array = &lit->value.array_data;
          free_expression_item_list(array->expr_list);
          break;
        }
        case String: {
          free(lit->value.string_data);
          break;
        }
        case Binary: {
          struct BinaryData* binary = &lit->value.binary;
          free(binary->buf);
          break;
        }
        case Integer:
        case Long:
        case Short:
        case Byte:
        case Float:
        case Double:
        case Boolean:
        case Timestamp:
        case TimestampNtz:
        case Date:
        case Decimal:
        case Null:
          break;
      }
      free(lit);
      break;
    };
    case Unary: {
      struct Unary* unary = ref.ref;
      free_expression_item_list(unary->sub_expr);
      free(unary);
      break;
    }
    case Column: {
      free((void*)ref.ref);
      break;
    }
  }
}

void free_expression_item_list(ExpressionItemList list)
{
  for (size_t i = 0; i < list.len; i++) {
    free_expression_item(list.exprList[i]);
  }
  free(list.exprList);
}
void print_n_spaces(int n)
{
  if (n == 0)
    return;
  printf("  ");
  print_n_spaces(n - 1);
}
void print_tree(ExpressionItem ref, int depth)
{
  switch (ref.type) {
    case BinOp: {
      struct BinOp* op = ref.ref;
      print_n_spaces(depth);
      switch (op->op) {
        case Add: {
          printf("Add\n");
          break;
        }
        case Minus: {
          printf("Minus\n");
          break;
        };
        case Divide: {
          printf("Divide\n");
          break;
        };
        case Multiply: {
          printf("Multiply\n");
          break;
        };
        case LessThan: {
          printf("LessThan\n");
          break;
        };
        case LessThanOrEqual: {
          printf("LessThanOrEqual\n");
          break;
        }
        case GreaterThan: {
          printf("GreaterThan\n");
          break;
        };
        case GreaterThaneOrEqual: {
          printf("GreaterThanOrEqual\n");
          break;
        };
        case Equal: {
          printf("Equal\n");
          break;
        };
        case NotEqual: {
          printf("NotEqual\n");
          break;
        };
        case In: {
          printf("In\n");
          break;
        };
        case NotIn: {
          printf("NotIn\n");
          break;
        }; break;
        case Distinct:
          printf("Distinct\n");
          break;
      }

      ExpressionItem left = op->exprs.exprList[0];
      ExpressionItem right = op->exprs.exprList[1];
      print_tree(left, depth + 1);
      print_tree(right, depth + 1);
      break;
    }
    case Variadic: {
      struct Variadic* var = ref.ref;
      print_n_spaces(depth);
      switch (var->op) {
        case And:
          printf("And\n");
          break;
        case Or:
          printf("Or\n");
          break;
        case StructExpression:
          printf("StructExpression\n");
          break;
        case ArrayData:
          printf("ArrayData\n");
          break;
      }
      for (size_t i = 0; i < var->expr_list.len; i++) {
        print_tree(var->expr_list.exprList[i], depth + 1);
      }
    } break;
    case Literal: {
      struct Literal* lit = ref.ref;
      print_n_spaces(depth);
      switch (lit->type) {
        case Integer:
          printf("Integer");
          printf("(%d)\n", lit->value.integer_data);
          break;
        case Long:
          printf("Long");
          printf("(%lld)\n", (long long)lit->value.long_data);
          break;
        case Short:
          printf("Short");
          printf("(%hd)\n", lit->value.short_data);
          break;
        case Byte:
          printf("Byte");
          printf("(%hhd)\n", lit->value.byte_data);
          break;
        case Float:
          printf("Float");
          printf("(%f)\n", (float)lit->value.float_data);
          break;
        case Double:
          printf("Double");
          printf("(%f)\n", lit->value.double_data);
          break;
        case String: {
          printf("String(%s)\n", lit->value.string_data);
          break;
        }
        case Boolean:
          printf("Boolean");
          printf("(%d)\n", lit->value.boolean_data);
          break;
        case Timestamp:
          printf("Timestamp");
          printf("(%lld)\n", (long long)lit->value.long_data);
          break;
        case TimestampNtz:
          printf("TimestampNtz");
          printf("(%lld)\n", (long long)lit->value.long_data);
          break;
        case Date:
          printf("Date");
          printf("(%d)\n", lit->value.integer_data);
          break;
        case Binary:
          printf("Binary\n");
          break;
        case Decimal: {
          struct Decimal* dec = &lit->value.decimal;
          printf(
            "Decimal(%lld,%lld, %d, %d)\n",
            (long long)dec->value[0],
            (long long)dec->value[1],
            dec->scale,
            dec->precision);
          break;
        }
        case Null:
          printf("Null\n");
          break;
        case Struct:
          printf("Struct\n");
          struct Struct* struct_data = &lit->value.struct_data;
          for (size_t i = 0; i < struct_data->values.len; i++) {
            print_n_spaces(depth + 1);

            // Extract field name from field
            ExpressionItem item = struct_data->fields.exprList[i];
            assert(item.type == Literal);
            struct Literal* lit = item.ref;
            assert(lit->type == String);

            printf("Field: %s\n", lit->value.string_data);
            print_tree(struct_data->values.exprList[i], depth + 2);
          }
          break;
        case Array:
          printf("Array\n");
          struct ArrayData* array = &lit->value.array_data;
          for (size_t i = 0; i < array->expr_list.len; i++) {
            print_tree(array->expr_list.exprList[i], depth + 1);
          }
          break;
      }
    } break;
    case Unary: {
      print_n_spaces(depth);
      struct Unary* unary = ref.ref;
      switch (unary->type) {
        case Not:
          printf("Not\n");
          break;
        case IsNull:
          printf("IsNull\n");
          break;
      }
      print_tree(unary->sub_expr.exprList[0], depth + 1);
      break;
    }
    case Column:
      print_n_spaces(depth);
      char* column_name = ref.ref;
      printf("Column(%s)\n", column_name);
      break;
  }
}
