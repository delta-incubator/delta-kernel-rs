#include "assert.h"
#include "delta_kernel_ffi.h"
#include "read_table.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DECL_BINOP(fun_name, op)                                                                   \
  uintptr_t fun_name(void* data, uintptr_t a, uintptr_t b)                                         \
  {                                                                                                \
    return visit_binop(data, a, b, op);                                                            \
  }
#define DECL_SIMPLE_SCALAR(fun_name, enum_member, c_type)                                          \
  uintptr_t fun_name(void* data, c_type val)                                                       \
  {                                                                                                \
    struct Literal* lit = malloc(sizeof(struct Literal));                                          \
    lit->type = enum_member;                                                                       \
    lit->value = (uintptr_t)val;                                                                   \
    return put_handle(data, lit, Literal);                                                         \
  }                                                                                                \
  _Static_assert(                                                                                  \
    sizeof(c_type) <= sizeof(uintptr_t), "The provided type is not a valid simple scalar")
#define DECL_VARIADIC(fun_name, enum_member)                                                       \
  uintptr_t fun_name(void* data, uintptr_t len)                                                    \
  {                                                                                                \
    return visit_variadic(data, len, enum_member);                                                 \
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
struct Literal
{
  enum LitType type;
  int64_t value;
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
enum ExpressionType
{
  BinOp,
  Variadic,
  Literal,
  BinaryLiteral,
  DecimalLiteral,
  StructLiteral,
  NullLiteral
};
struct Variadic
{
  enum VariadicType op;
  size_t len;
  size_t max_len;
  struct ExpressionRef* expr_list;
};
struct Binary
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
struct ExpressionRef
{
  void* ref;
  enum ExpressionType type;
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

uintptr_t visit_binop(void* data, uintptr_t a, uintptr_t b, enum OpType op)
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
DECL_BINOP(visit_add, Add)
DECL_BINOP(visit_minus, Sub)
DECL_BINOP(visit_multiply, Mul)
DECL_BINOP(visit_divide, Div)
DECL_BINOP(visit_lt, LT)
DECL_BINOP(visit_le, LE)
DECL_BINOP(visit_gt, GT)
DECL_BINOP(visit_ge, GE)
DECL_BINOP(visit_eq, EQ)
DECL_BINOP(visit_ne, NE)
DECL_BINOP(visit_distinct, Distinct)
DECL_BINOP(visit_in, In)
DECL_BINOP(visit_not_in, NotIn)

uintptr_t visit_expr_decimal(
  void* data,
  uint64_t value_ms,
  uint64_t value_ls,
  uint8_t precision,
  uint8_t scale)
{
  struct Decimal* dec = malloc(sizeof(struct Decimal));
  dec->value[0] = value_ms;
  dec->value[1] = value_ls;
  dec->precision = precision;
  dec->scale = scale;
  return put_handle(data, dec, DecimalLiteral);
}
DECL_SIMPLE_SCALAR(visit_expr_int, Integer, int32_t);
DECL_SIMPLE_SCALAR(visit_expr_long, Long, int64_t);
DECL_SIMPLE_SCALAR(visit_expr_short, Long, int16_t);
DECL_SIMPLE_SCALAR(visit_expr_byte, Byte, int8_t);
DECL_SIMPLE_SCALAR(visit_expr_float, Float, float);
DECL_SIMPLE_SCALAR(visit_expr_double, Double, double);
DECL_SIMPLE_SCALAR(visit_expr_boolean, Boolean, _Bool);
DECL_SIMPLE_SCALAR(visit_expr_timestamp, Timestamp, int64_t);
DECL_SIMPLE_SCALAR(visit_expr_timestamp_ntz, TimestampNtz, int64_t);
DECL_SIMPLE_SCALAR(visit_expr_date, Date, int32_t);

uintptr_t visit_variadic(void* data, uintptr_t len, enum VariadicType op)
{
  struct Variadic* var = malloc(sizeof(struct Variadic));
  struct ExpressionRef* expr_lst = malloc(sizeof(struct ExpressionRef) * len);
  var->op = op;
  var->len = 0;
  var->max_len = len;
  var->expr_list = expr_lst;
  return put_handle(data, var, Variadic);
}
void visit_variadic_item(void* data, uintptr_t variadic_id, uintptr_t sub_expr_id)
{
  struct ExpressionRef* sub_expr_ref = get_handle(data, sub_expr_id);
  struct ExpressionRef* variadic_ref = get_handle(data, variadic_id);
  assert(sub_expr_ref != NULL && variadic_ref != NULL);
  assert(variadic_ref->type == Variadic);

  struct Variadic* variadic = variadic_ref->ref;
  variadic->expr_list[variadic->len++] = *sub_expr_ref;
}
DECL_VARIADIC(visit_and, And)
DECL_VARIADIC(visit_or, Or)
DECL_VARIADIC(visit_struct_constructor, StructConstructor)
DECL_VARIADIC(visit_expr_array, ArrayData)

uintptr_t visit_expr_binary(void* data, const uint8_t* buf, uintptr_t len)
{
  struct Binary* bin = malloc(sizeof(struct Binary));
  bin->buf = malloc(len);
  memcpy(bin->buf, buf, len);
  return put_handle(data, bin, BinaryLiteral);
}

uintptr_t visit_expr_struct(void* data, uintptr_t len)
{
  struct Struct* struct_data = malloc(sizeof(struct Struct));
  struct_data->len = 0;
  struct_data->max_len = len;
  struct_data->expressions = malloc(sizeof(struct ExpressionRef) * len);
  struct_data->field_names = malloc(sizeof(KernelStringSlice) * len);
  return put_handle(data, struct_data, StructLiteral);
}

void visit_expr_struct_field(
  void* data,
  uintptr_t struct_id,
  KernelStringSlice field_name,
  uintptr_t value_id)
{
  struct ExpressionRef* value = get_handle(data, value_id);
  struct ExpressionRef* struct_handle = get_handle(data, struct_id);
  assert(struct_handle != NULL && value != NULL);
  assert(struct_handle->type == StructLiteral);

  struct Struct* struct_ref = (struct Struct*)struct_handle->ref;
  size_t len = struct_ref->len;
  assert(len < struct_ref->max_len);

  struct_ref->expressions[len] = *value;
  struct_ref->field_names[len] = field_name;
  struct_ref->len++;
}

uintptr_t visit_null(void* data)
{
  return put_handle(data, NULL, NullLiteral);
}

// Print the schema of the snapshot
struct ExpressionRef construct_predicate(KernelPredicate* predicate)
{
  print_diag("Building schema\n");
  struct Data data = { 0 };
  EngineExpressionVisitor visitor = { .data = &data,
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
                                      .visit_string = NULL,
                                      .visit_and = visit_and,
                                      .visit_or = visit_or,
                                      .visit_variadic_item = visit_variadic_item,
                                      .visit_not = NULL,
                                      .visit_is_null = NULL,
                                      .visit_lt = visit_lt,
                                      .visit_le = visit_le,
                                      .visit_gt = visit_gt,
                                      .visit_ge = visit_ge,
                                      .visit_eq = visit_eq,
                                      .visit_ne = visit_ne,
                                      .visit_distinct = visit_distinct,
                                      .visit_in = visit_in,
                                      .visit_not_in = visit_not_in,
                                      .visit_add = visit_add,
                                      .visit_minus = visit_minus,
                                      .visit_multiply = visit_multiply,
                                      .visit_divide = visit_divide,
                                      .visit_column = NULL,
                                      .visit_expr_struct = visit_struct_constructor,
                                      .visit_expr_struct_item =
                                        visit_variadic_item, // treating expr struct like a variadic
                                      .visit_null = visit_null,
                                      .visit_struct = visit_expr_struct,
                                      .visit_struct_field = visit_expr_struct_field,
                                      .visit_array = visit_expr_array,
                                      .visit_array_item = visit_variadic_item };
  uintptr_t schema_list_id = visit_expression(&predicate, &visitor);
  return data.handles[schema_list_id];
}

void tab_helper(int n)
{
  if (n == 0)
    return;
  printf("  ");
  tab_helper(n - 1);
}

void print_tree(struct ExpressionRef ref, int depth)
{
  switch (ref.type) {
    case BinOp: {
      struct BinOp* op = ref.ref;
      tab_helper(depth);
      switch (op->op) {
        case Add: {
          printf("ADD \n");
          break;
        }
        case Sub: {
          printf("SUB \n");
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
          printf("Distinct");
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
      tab_helper(depth);
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
      tab_helper(depth);
      switch (lit->type) {
        case Integer:
          printf("Integer");
          break;
        case Short:
          printf("Short");
          break;
        case Byte:
          printf("Byte");
          break;
        case Float:
          printf("Float");
          break;
        case Double:
          printf("Double");
          break;
        case String:
          printf("String");
          break;
        case Boolean:
          printf("Boolean");
          break;
        case Timestamp:
          printf("Timestamp");
          break;
        case TimestampNtz:
          printf("TimestampNtz");
          break;
        case Date:
          printf("Date");
          break;
        case Binary:
          printf("Binary");
          break;
        case Decimal:
          printf("Decimal");
          break;
        case Null:
          printf("Null");
          break;
        case Struct:
          printf("Struct");
          break;
        case Array:
          printf("Array");
          break;
        case Long:
          printf("Long");
          break;
      }
      printf("(%lld)\n", lit->value);
    } break;
    case BinaryLiteral:
      tab_helper(depth);
      printf("BinaryLiteral\n");
    case DecimalLiteral:
      tab_helper(depth);
      printf("DecimalLiteral\n");
      break;
    case StructLiteral:
      tab_helper(depth);
      struct Struct* struct_data = ref.ref;
      printf("Struct\n");
      for (size_t i = 0; i < struct_data->len; i++) {
        tab_helper(depth);
        printf("Field: %s\n", struct_data->field_names[i].ptr);
        print_tree(struct_data->expressions[i], depth + 1);
      }
      break;
    case NullLiteral:
      tab_helper(depth);
      printf("Null\n");
      break;
  }
}

void test_kernel_expr()
{
  KernelPredicate* pred = get_kernel_expression();
  struct ExpressionRef ref = construct_predicate(pred);
  print_tree(ref, 0);
}
