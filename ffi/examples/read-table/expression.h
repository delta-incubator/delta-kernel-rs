#include "assert.h"
#include "delta_kernel_ffi.h"
#include "read_table.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFINE_BINOP(fun_name, op)                                                                 \
  uintptr_t fun_name(void* data, uintptr_t a, uintptr_t b)                                         \
  {                                                                                                \
    return visit_expr_binop(data, a, b, op);                                                       \
  }
#define DEFINE_SIMPLE_SCALAR(fun_name, enum_member, c_type)                                        \
  uintptr_t fun_name(void* data, c_type val)                                                       \
  {                                                                                                \
    struct Literal* lit = malloc(sizeof(struct Literal));                                          \
    lit->type = enum_member;                                                                       \
    lit->value.simple = (uintptr_t)val;                                                            \
    return put_handle(data, lit, Literal);                                                         \
  }                                                                                                \
  _Static_assert(                                                                                  \
    sizeof(c_type) <= sizeof(uintptr_t), "The provided type is not a valid simple scalar")
#define DEFINE_VARIADIC(fun_name, enum_member)                                                     \
  uintptr_t fun_name(void* data, uintptr_t len)                                                    \
  {                                                                                                \
    return visit_expr_variadic(data, len, enum_member);                                            \
  }
#define DEFINE_UNARY(fun_name, op)                                                                 \
  uintptr_t fun_name(void* data, uintptr_t sub_expr)                                               \
  {                                                                                                \
    return visit_expr_unary(data, sub_expr, op);                                                   \
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
struct ExpressionRef
{
  void* ref;
  enum ExpressionType type;
};
struct BinOp
{
  enum OpType op;
  struct Literal* left;
  struct Literal* right;
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
  size_t len;
  size_t max_len;
  struct ExpressionRef* expr_list;
};
struct Unary
{
  enum UnaryType type;
  struct ExpressionRef sub_expr;
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
struct Data
{
  size_t len;
  struct ExpressionRef handles[100];
};
struct Struct
{
  KernelStringSlice* field_names;
  struct ExpressionRef* expressions;
  size_t len;
  size_t max_len;
};

struct ArrayData
{
  size_t len;
  size_t max_len;
  struct ExpressionRef* expr_list;
};

struct Literal
{
  enum LitType type;
  union LiteralValue
  {
    uint64_t simple;
    struct KernelStringSlice string_data;
    struct Struct struct_data;
    struct ArrayData array_data;
    struct BinaryData binary;
    struct Decimal decimal;
  } value;
};

size_t put_handle(void* data, void* ref, enum ExpressionType type)
{
  struct Data* data_ptr = (struct Data*)data;
  struct ExpressionRef expr = { .ref = ref, .type = type };
  data_ptr->handles[data_ptr->len] = expr;
  return data_ptr->len++;
}
struct ExpressionRef* get_handle(void* data, size_t handle_index)
{
  struct Data* data_ptr = (struct Data*)data;
  if (handle_index > data_ptr->len) {
    return NULL;
  }
  return &data_ptr->handles[handle_index];
}
KernelStringSlice copy_kernel_string(KernelStringSlice string)
{
  char* contents = malloc(string.len + 1);
  strncpy(contents, string.ptr, string.len);
  contents[string.len] = '\0';
  KernelStringSlice out = { .len = string.len, .ptr = contents };
  return out;
}

uintptr_t visit_expr_binop(void* data, uintptr_t a, uintptr_t b, enum OpType op)
{
  struct BinOp* binop = malloc(sizeof(struct BinOp));
  struct ExpressionRef* left_handle = get_handle(data, a);
  struct ExpressionRef* right_handle = get_handle(data, b);
  assert(right_handle != NULL && left_handle != NULL);

  struct Literal* left = left_handle->ref;
  struct Literal* right = right_handle->ref;
  binop->op = op;
  binop->left = left;
  binop->right = right;
  return put_handle(data, binop, BinOp);
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

uintptr_t visit_expr_string(void* data, KernelStringSlice string)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = String;
  literal->value.string_data = copy_kernel_string(string);
  return put_handle(data, literal, Literal);
}

uintptr_t visit_expr_decimal(
  void* data,
  uint64_t value_ms,
  uint64_t value_ls,
  uint8_t precision,
  uint8_t scale)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Decimal;
  struct Decimal* dec = &literal->value.decimal;
  dec->value[0] = value_ms;
  dec->value[1] = value_ls;
  dec->precision = precision;
  dec->scale = scale;
  return put_handle(data, literal, Literal);
}
DEFINE_SIMPLE_SCALAR(visit_expr_int, Integer, int32_t);
DEFINE_SIMPLE_SCALAR(visit_expr_long, Long, int64_t);
DEFINE_SIMPLE_SCALAR(visit_expr_short, Short, int16_t);
DEFINE_SIMPLE_SCALAR(visit_expr_byte, Byte, int8_t);
DEFINE_SIMPLE_SCALAR(visit_expr_float, Float, float);
DEFINE_SIMPLE_SCALAR(visit_expr_double, Double, double);
DEFINE_SIMPLE_SCALAR(visit_expr_boolean, Boolean, _Bool);
DEFINE_SIMPLE_SCALAR(visit_expr_timestamp, Timestamp, int64_t);
DEFINE_SIMPLE_SCALAR(visit_expr_timestamp_ntz, TimestampNtz, int64_t);
DEFINE_SIMPLE_SCALAR(visit_expr_date, Date, int32_t);

uintptr_t visit_expr_variadic(void* data, uintptr_t len, enum VariadicType op)
{
  struct Variadic* var = malloc(sizeof(struct Variadic));
  struct ExpressionRef* expr_lst = malloc(sizeof(struct ExpressionRef) * len);
  var->op = op;
  var->len = 0;
  var->max_len = len;
  var->expr_list = expr_lst;
  return put_handle(data, var, Variadic);
}
void visit_expr_variadic_item(void* data, uintptr_t variadic_id, uintptr_t sub_expr_id)
{
  struct ExpressionRef* sub_expr_ref = get_handle(data, sub_expr_id);
  struct ExpressionRef* variadic_ref = get_handle(data, variadic_id);
  assert(sub_expr_ref != NULL && variadic_ref != NULL);
  assert(variadic_ref->type == Variadic);

  struct Variadic* variadic = variadic_ref->ref;
  variadic->expr_list[variadic->len++] = *sub_expr_ref;
}
DEFINE_VARIADIC(visit_expr_and, And)
DEFINE_VARIADIC(visit_expr_or, Or)
DEFINE_VARIADIC(visit_expr_struct, StructConstructor)

void visit_expr_array_item(void* data, uintptr_t variadic_id, uintptr_t sub_expr_id)
{
  struct ExpressionRef* sub_expr_handle = get_handle(data, sub_expr_id);
  struct ExpressionRef* array_handle = get_handle(data, variadic_id);
  assert(sub_expr_handle != NULL && array_handle != NULL);
  assert(array_handle->type == Literal);
  struct Literal* literal = array_handle->ref;
  assert(literal->type == Array);
  struct ArrayData* array = &literal->value.array_data;
  array->expr_list[array->len++] = *sub_expr_handle;
}
uintptr_t visit_expr_array(void* data, uintptr_t len)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Array;
  struct ArrayData* arr = &(literal->value.array_data);
  arr->len = 0;
  arr->max_len = 0;
  arr->expr_list = malloc(sizeof(struct ExpressionRef) * len);
  return put_handle(data, literal, Literal);
}

uintptr_t visit_expr_binary(void* data, const uint8_t* buf, uintptr_t len)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Binary;
  struct BinaryData* bin = &literal->value.binary;
  bin->buf = malloc(len);
  memcpy(bin->buf, buf, len);
  return put_handle(data, literal, Literal);
}

uintptr_t visit_expr_struct_literal(void* data, uintptr_t len)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Struct;
  struct Struct* struct_data = &literal->value.struct_data;
  struct_data->len = 0;
  struct_data->max_len = len;
  struct_data->expressions = malloc(sizeof(struct ExpressionRef) * len);
  struct_data->field_names = malloc(sizeof(KernelStringSlice) * len);
  return put_handle(data, literal, Literal);
}

void visit_expr_struct_literal_field(
  void* data,
  uintptr_t struct_id,
  KernelStringSlice field_name,
  uintptr_t value_id)
{
  struct ExpressionRef* value = get_handle(data, value_id);
  struct ExpressionRef* literal_handle = get_handle(data, struct_id);
  assert(literal_handle != NULL && value != NULL);
  assert(literal_handle->type == Literal);
  struct Literal* literal = literal_handle->ref;
  assert(literal->type == Struct);

  struct Struct* struct_ref = &literal->value.struct_data;
  size_t len = struct_ref->len;
  assert(len < struct_ref->max_len);

  struct_ref->expressions[len] = *value;
  struct_ref->field_names[len] = copy_kernel_string(field_name);
  struct_ref->len++;
}

uintptr_t visit_expr_null(void* data)
{
  struct Literal* literal = malloc(sizeof(struct Literal));
  literal->type = Null;
  return put_handle(data, literal, Literal);
}

uintptr_t visit_expr_unary(void* data, uintptr_t sub_expr_id, enum UnaryType type)
{
  struct Unary* unary = malloc(sizeof(struct Unary));
  unary->type = type;
  struct ExpressionRef* sub_expr_handle = get_handle(data, sub_expr_id);
  unary->sub_expr = *sub_expr_handle;
  return put_handle(data, unary, Unary);
}
DEFINE_UNARY(visit_expr_is_null, IsNull)
DEFINE_UNARY(visit_expr_not, Not)

uintptr_t visit_expr_column(void* data, KernelStringSlice string)
{
  struct KernelStringSlice* heap_string = malloc(sizeof(KernelStringSlice));
  *heap_string = copy_kernel_string(string);
  printf("Creating column with len %lu: %s\n", string.len, heap_string->ptr);
  return put_handle(data, heap_string, Column);
}

// Print the schema of the snapshot
struct ExpressionRef construct_predicate(KernelPredicate* predicate)
{
  print_diag("Building schema\n");
  struct Data data = { 0 };
  EngineExpressionVisitor visitor = {
    .data = &data,
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
    .visit_variadic_item = visit_expr_variadic_item,
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
    .visit_struct_item = visit_expr_variadic_item, // We treat expr struct as a variadic
    .visit_null = visit_expr_null,
    .visit_struct_literal = visit_expr_struct_literal,
    .visit_struct_literal_field = visit_expr_struct_literal_field,
    .visit_array = visit_expr_array,
    .visit_array_item = visit_expr_array_item
  };
  uintptr_t schema_list_id = visit_expression(&predicate, &visitor);
  return data.handles[schema_list_id];
}

void print_n_spaces(int n)
{
  if (n == 0)
    return;
  printf("  ");
  print_n_spaces(n - 1);
}
void free_expression(struct ExpressionRef ref)
{
  switch (ref.type) {
    case BinOp: {
      struct BinOp* op = ref.ref;
      struct ExpressionRef left = { .ref = op->left, .type = Literal };
      struct ExpressionRef right = { .ref = op->right, .type = Literal };
      free_expression(left);
      free_expression(right);
      free(op);
      break;
    }
    case Variadic: {
      struct Variadic* var = ref.ref;
      for (size_t i = 0; i < var->len; i++) {
        free_expression(var->expr_list[i]);
      }
      free(var);
      break;
    };
    case Literal: {
      struct Literal* lit = ref.ref;
      switch (lit->type) {
        case Struct: {
          struct Struct* struct_data = &lit->value.struct_data;
          for (size_t i = 0; i < struct_data->len; i++) {
            free((void*)struct_data->field_names[i].ptr);
          }
          break;
        }
        case Array: {
          struct ArrayData* array = &lit->value.array_data;
          for (size_t i = 0; i < array->len; i++) {
            free_expression(array->expr_list[i]);
          }
          free(array->expr_list);
          break;
        }
        case String: {
          struct KernelStringSlice* string = &lit->value.string_data;
          free((void*)string->ptr);
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
        case Binary:
        case Decimal:
        case Null:
          break;
      }
      free(lit);
      break;
    };
    case Unary: {
      struct Unary* unary = ref.ref;
      free_expression(unary->sub_expr);
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
void print_tree(struct ExpressionRef ref, int depth)
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

      struct ExpressionRef left = { .ref = op->left, .type = Literal };
      struct ExpressionRef right = { .ref = op->right, .type = Literal };
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
      for (size_t i = 0; i < var->len; i++) {
        print_tree(var->expr_list[i], depth + 1);
      }
    } break;
    case Literal: {
      struct Literal* lit = ref.ref;
      print_n_spaces(depth);
      switch (lit->type) {
        case Integer:
          printf("Integer");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Long:
          printf("Long");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Short:
          printf("Short");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Byte:
          printf("Byte");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Float:
          printf("Float");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Double:
          printf("Double");
          printf("(%lld)\n", lit->value.simple);
          break;
        case String: {
          printf("String(%s)\n", lit->value.string_data.ptr);
          break;
        }
        case Boolean:
          printf("Boolean");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Timestamp:
          printf("Timestamp");
          printf("(%lld)\n", lit->value.simple);
          break;
        case TimestampNtz:
          printf("TimestampNtz");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Date:
          printf("Date");
          printf("(%lld)\n", lit->value.simple);
          break;
        case Binary:
          printf("Binary\n");
          break;
        case Decimal:
          printf("Decimal\n");
          break;
        case Null:
          printf("Null\n");
          break;
        case Struct:
          printf("Struct\n");
          struct Struct* struct_data = &lit->value.struct_data;
          for (size_t i = 0; i < struct_data->len; i++) {
            print_n_spaces(depth + 1);
            printf("Field: %s\n", struct_data->field_names[i].ptr);
            print_tree(struct_data->expressions[i], depth + 2);
          }
          break;
        case Array:
          printf("Array\n");
          struct ArrayData* array = &lit->value.array_data;
          for (size_t i = 0; i < array->len; i++) {
            print_tree(array->expr_list[i], depth + 1);
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
      print_tree(unary->sub_expr, depth + 1);
      break;
    }
    case Column:
      print_n_spaces(depth);
      KernelStringSlice* string = ref.ref;
      printf("Column(%s)\n", string->ptr);
      break;
  }
}

void test_kernel_expr()
{
  KernelPredicate* pred = get_kernel_expression();
  struct ExpressionRef ref = construct_predicate(pred);
  print_tree(ref, 0);
  free_expression(ref);
}
