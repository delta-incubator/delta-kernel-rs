#include "assert.h"
#include "delta_kernel_ffi.h"
#include "read_table.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

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

enum VariadicType
{
  And,
  Or
};
enum ExpressionType
{
  BinOp,
  Variadic,
  Literal
};
struct Variadic
{
  enum VariadicType op;
  size_t len;
  size_t max_len;
  struct ExpressionRef* expr_list;
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
  binop->op = op;
  binop->left = (struct Literal*)a;
  binop->right = (struct Literal*)b;
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

DECL_SIMPLE_SCALAR(visit_expr_int, Integer, int32_t);
DECL_SIMPLE_SCALAR(visit_expr_long, Long, int64_t);
DECL_SIMPLE_SCALAR(visit_expr_short, Long, int16_t);
DECL_SIMPLE_SCALAR(visit_expr_byte, Byte, int8_t);
DECL_SIMPLE_SCALAR(visit_expr_float, Float, float);
DECL_SIMPLE_SCALAR(visit_expr_double, Double, double);
DECL_SIMPLE_SCALAR(visit_expr_boolean, Boolean, _Bool);
DECL_SIMPLE_SCALAR(visit_expr_timestamp, Timestamp, int64_t);
DECL_SIMPLE_SCALAR(visit_expr_timestamp_ntz, TimestampNtz, int64_t);
DECL_SIMPLE_SCALAR(visit_expr_date, Date, int64_t);

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
  if (sub_expr_ref == NULL || variadic_ref == NULL) {
    abort();
  }
  struct Variadic* variadic = variadic_ref->ref;
  variadic->expr_list[variadic->len++] = *sub_expr_ref;
}
uintptr_t visit_and(void* data, uintptr_t len)
{
  return visit_variadic(data, len, And);
}
uintptr_t visit_or(void* data, uintptr_t len)
{
  return visit_variadic(data, len, Or);
}

// Print the schema of the snapshot
struct ExpressionRef construct_predicate(KernelPredicate* predicate)
{
  print_diag("Building schema\n");
  struct Data data = { 0 };
  EngineExpressionVisitor visitor = {
    .data = &data,
    .make_expr_list = NULL,
    .visit_int = visit_expr_int,
    .visit_long = visit_expr_long,
    .visit_short = visit_expr_short,
    .visit_byte = visit_expr_byte,
    .visit_float = visit_expr_float,
    .visit_double = visit_expr_double,
    .visit_bool = visit_expr_boolean,
    .visit_timestamp = visit_expr_timestamp,
    .visit_timestamp_ntz = visit_expr_timestamp_ntz,
    .visit_date = NULL,
    .visit_binary = NULL,
    .visit_decimal = NULL,
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
    .visit_expr_struct = NULL,
    .visit_expr_struct_item = NULL,
  };
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
          printf("AND (\n");
          break;
        case Or:
          printf("OR (\n");
          break;
      }
      for (size_t i = 0; i < var->len; i++) {
        print_tree(var->expr_list[i], depth + 1);
      }
      tab_helper(depth);
      printf(")\n");
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
  }
}

void test_kernel_expr()
{
  KernelPredicate* pred = get_kernel_expression();
  struct ExpressionRef ref = construct_predicate(pred);
  print_tree(ref, 0);
}
