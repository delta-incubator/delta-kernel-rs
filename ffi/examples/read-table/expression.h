#include "assert.h"
#include "delta_kernel_ffi.h"
#include "read_table.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/**
 * This module defines a very simple model of an expression, used only to be able to print the
 * provided expression. It consists of an "ExpressionBuilder" which is our user data that gets
 * passed into each visit_x call. This simply keeps track of all the expressions we are asked to
 * allocate.
 *
 * Each expression is an "ExpressionItem", which tracks the type and pointer to the expression.
 */

#define DEFINE_BINOP(fun_name, op)                                                                 \
  void fun_name(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)                    \
  {                                                                                                \
    visit_expr_binop(data, op, child_list_id, sibling_list_id);                                    \
  }
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
#define DEFINE_VARIADIC(fun_name, enum_member)                                                     \
  void fun_name(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)                    \
  {                                                                                                \
    visit_expr_variadic(data, enum_member, child_list_id, sibling_list_id);                        \
  }
#define DEFINE_UNARY(fun_name, op)                                                                 \
  void fun_name(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)                    \
  {                                                                                                \
    visit_expr_unary(data, op, child_list_id, sibling_list_id);                                    \
  }
enum OpType
{
  Add,
  Sub,
  Div,
  Mul,
  LT,
  LE,
  GT,
  GE,
  EQ,
  NE,
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
  StructConstructor,
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
    struct KernelStringSlice string_data;
    struct Struct struct_data;
    struct ArrayData array_data;
    struct BinaryData binary;
    struct Decimal decimal;
  } value;
};

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
KernelStringSlice copy_kernel_string(KernelStringSlice string)
{
  char* contents = malloc(string.len + 1);
  strncpy(contents, string.ptr, string.len);
  contents[string.len] = '\0';
  KernelStringSlice out = { .len = string.len, .ptr = contents };
  return out;
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
DEFINE_BINOP(visit_expr_minus, Sub)
DEFINE_BINOP(visit_expr_multiply, Mul)
DEFINE_BINOP(visit_expr_divide, Div)
DEFINE_BINOP(visit_expr_lt, LT)
DEFINE_BINOP(visit_expr_le, LE)
DEFINE_BINOP(visit_expr_gt, GT)
DEFINE_BINOP(visit_expr_ge, GE)
DEFINE_BINOP(visit_expr_eq, EQ)
DEFINE_BINOP(visit_expr_ne, NE)
DEFINE_BINOP(visit_expr_distinct, Distinct)
DEFINE_BINOP(visit_expr_in, In)
DEFINE_BINOP(visit_expr_not_in, NotIn)

void visit_expr_string(void* data, KernelStringSlice string, uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = String;
  literal->value.string_data = copy_kernel_string(string);
  put_handle(data, literal, Literal, sibling_list_id);
}

void visit_expr_decimal(
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
DEFINE_SIMPLE_SCALAR(visit_expr_int, Integer, int32_t, integer_data);
DEFINE_SIMPLE_SCALAR(visit_expr_long, Long, int64_t, long_data);
DEFINE_SIMPLE_SCALAR(visit_expr_short, Short, int16_t, short_data);
DEFINE_SIMPLE_SCALAR(visit_expr_byte, Byte, int8_t, byte_data);
DEFINE_SIMPLE_SCALAR(visit_expr_float, Float, float, float_data);
DEFINE_SIMPLE_SCALAR(visit_expr_double, Double, double, double_data);
DEFINE_SIMPLE_SCALAR(visit_expr_boolean, Boolean, _Bool, boolean_data);
DEFINE_SIMPLE_SCALAR(visit_expr_timestamp, Timestamp, int64_t, long_data);
DEFINE_SIMPLE_SCALAR(visit_expr_timestamp_ntz, TimestampNtz, int64_t, long_data);
DEFINE_SIMPLE_SCALAR(visit_expr_date, Date, int32_t, integer_data);

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
DEFINE_VARIADIC(visit_expr_struct, StructConstructor)

void visit_expr_array(void* data, uintptr_t child_list_id, uintptr_t sibling_list_id)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Array;
  struct ArrayData* arr = &(literal->value.array_data);
  arr->expr_list = get_handle(data, child_list_id);
  put_handle(data, literal, Literal, sibling_list_id);
}

void visit_expr_binary(void* data, const uint8_t* buf, uintptr_t len, uintptr_t sibling_list_id)
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

void visit_expr_null(void* data, uintptr_t sibling_id_list)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Null;
  put_handle(data, literal, Literal, sibling_id_list);
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

void visit_expr_column(void* data, KernelStringSlice string, uintptr_t sibling_id_list)
{
  struct KernelStringSlice* heap_string = malloc(sizeof(KernelStringSlice));
  *heap_string = copy_kernel_string(string);
  put_handle(data, heap_string, Column, sibling_id_list);
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

// Print the schema of the snapshot
ExpressionItem construct_predicate(SharedExpression* predicate)
{
  ExpressionBuilder data = { 0 };
  data.lists = malloc(sizeof(ExpressionItem) * 100);
  EngineExpressionVisitor visitor = {
    .data = &data,
    .make_field_list = make_field_list,
    .visit_int = visit_expr_int,
    .visit_long = visit_expr_long,
    .visit_short = visit_expr_short,
    .visit_byte = visit_expr_byte,
    .visit_float = visit_expr_float,
    .visit_double = visit_expr_double,
    .visit_bool = visit_expr_boolean,
    .visit_timestamp = visit_expr_timestamp,
    .visit_timestamp_ntz = visit_expr_timestamp_ntz,
    .visit_date = visit_expr_date,
    .visit_binary = visit_expr_binary,
    .visit_decimal = visit_expr_decimal,
    .visit_string = visit_expr_string,
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
    .visit_struct = visit_expr_struct,
    .visit_null = visit_expr_null,
    .visit_struct_literal = visit_expr_struct_literal,
    .visit_array = visit_expr_array,
  };
  uintptr_t top_level_id = visit_expression(&predicate, &visitor);
  ExpressionItem ret = data.lists[top_level_id].exprList[0];
  return ret;
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
          struct KernelStringSlice* string = &lit->value.string_data;
          free((void*)string->ptr);
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
      KernelStringSlice* string = ref.ref;
      free((void*)string->ptr);
      free(string);
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
          printf("ADD\n");
          break;
        }
        case Sub: {
          printf("SUB\n");
          break;
        };
        case Div: {
          printf("DIV\n");
          break;
        };
        case Mul: {
          printf("MUL\n");
          break;
        };
        case LT: {
          printf("LT\n");
          break;
        };
        case LE: {
          printf("LE\n");
          break;
        }
        case GT: {
          printf("GT\n");
          break;
        };
        case GE: {
          printf("GE\n");
          break;
        };
        case EQ: {
          printf("EQ\n");
          break;
        };
        case NE: {
          printf("NE\n");
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
        case StructConstructor:
          printf("StructConstructor\n");
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
          printf("String(%s)\n", lit->value.string_data.ptr);
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
            printf("Field\n");
            print_tree(struct_data->fields.exprList[i], depth + 2);
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
      KernelStringSlice* string = ref.ref;
      printf("Column(%s)\n", string->ptr);
      break;
  }
}
