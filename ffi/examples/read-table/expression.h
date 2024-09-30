#include "delta_kernel_ffi.h"
#include "assert.h"
#include "read_table.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

enum OpType {
  Add,
  Sub
};
enum LitType {
  i32,
  i16,
  i8
};
struct Literal {
  enum LitType type;
  int64_t value;
};
struct BinOp {
  enum OpType op;
  struct Literal *left;
  struct Literal *right;
};

enum VariadicType {
  And,
  Or
};
enum ExpressionType {
  BinOp,
  Variadic,
  Literal
};
struct Variadic {
  enum VariadicType op;
  size_t len;
  size_t max_len;
  struct ExpressionRef *expr_list;
};
struct ExpressionRef {
  void* ref;
  enum ExpressionType type;
};
struct Data {
  size_t len;
  struct ExpressionRef handles[100];
};

size_t put_handle(void *data, void *ref, enum ExpressionType type) {
    struct Data * data_ptr = (struct Data *) data;
    struct ExpressionRef expr = {.ref= ref, .type = type};
    data_ptr->handles[data_ptr->len] = expr;
    return data_ptr->len++;
}
struct ExpressionRef *get_handle(void *data, size_t handle_index) {
  struct Data * data_ptr = (struct Data *) data;
  if (handle_index > data_ptr->len) {
    return NULL;
  }
  return &data_ptr->handles[handle_index];
}

uintptr_t visit_add(void *data, uintptr_t a, uintptr_t b) {
    struct BinOp *op = malloc(sizeof(struct BinOp));
    op->op = Add;
    op->left = (struct Literal *) a;
    op->right = (struct Literal *) b;
    return put_handle(data, op, BinOp);
}

uintptr_t visit_int(void *data, int32_t a) {
  struct Literal *val = malloc(sizeof(struct Literal));
  val->type = i32;
  val->value = (uintptr_t) a;
  return put_handle(data, val, Literal);
}

uintptr_t visit_and(void *data, uintptr_t len) {
  struct Variadic* and = malloc(sizeof(struct Variadic));
  struct ExpressionRef* expr_lst = malloc(sizeof(struct ExpressionRef) * len);
  and->len = 0;
  and->max_len = len;
  and->expr_list = expr_lst;
  return put_handle(data, and, Variadic);
}

void visit_variadic_item(void *data, uintptr_t variadic_id, uintptr_t sub_expr_id) {
  struct ExpressionRef *sub_expr_ref = get_handle(data, sub_expr_id);
  struct ExpressionRef *variadic_ref = get_handle(data, variadic_id);
  if (sub_expr_ref == NULL || variadic_ref == NULL) {
    abort();
  }
  struct Variadic *variadic = variadic_ref->ref;
  variadic->expr_list[variadic->len++] = *sub_expr_ref;
}

// Print the schema of the snapshot
struct ExpressionRef construct_predicate(KernelPredicate* predicate)
{
  print_diag("Building schema\n");
  struct Data data = {0};
  EngineExpressionVisitor visitor = {
    .data = &data,
    .make_expr_list = NULL,
    .visit_int = visit_int,
    .visit_long = NULL,
    .visit_short = NULL,
    .visit_byte = NULL,
    .visit_float = NULL,
    .visit_double = NULL,
    .visit_bool = NULL,
    .visit_string = NULL,
    .visit_and = visit_and,
    .visit_or = NULL,
    .visit_variadic_item = visit_variadic_item,
    .visit_not = NULL,
    .visit_is_null = NULL,
    .visit_lt = NULL,
    .visit_le = NULL,
    .visit_gt = NULL,
    .visit_ge = NULL,
    .visit_eq = NULL,
    .visit_ne = NULL,
    .visit_distinct = NULL,
    .visit_in = NULL,
    .visit_not_in = NULL,
    .visit_add = visit_add,
    .visit_minus = NULL,
    .visit_multiply = NULL,
    .visit_divide = NULL,
    .visit_column = NULL,
  };
  uintptr_t schema_list_id = visit_expression(&predicate, &visitor);
  return data.handles[schema_list_id];
}

void tab_helper(int n) {
  if (n == 0) return;
  printf("  ");
  tab_helper(n-1);
}

void print_tree(struct ExpressionRef ref, int depth) {
  switch (ref.type) {
    case BinOp: {
      struct BinOp *op = ref.ref;
      tab_helper(depth);
      switch(op->op) {
        case Add:{
          printf("ADD \n");
          break;
        }
        case Sub: {
          printf("SUB \n");
          break;
        }
        break;
      }

      struct ExpressionRef left = {.ref = op->left, .type = Literal};
      struct ExpressionRef right = {.ref = op->right, .type = Literal};
      print_tree(left, depth+1);
      print_tree(right, depth+1);
      break;
    }
    case Variadic: {
      struct Variadic *var = ref.ref;
      tab_helper(depth);
      switch (var->op) {
        case And:
          printf("AND (\n");
          break;
        case Or:
          printf("OR (\n");
          break;
      }
      for (size_t i = 0; i < var->len; i ++) {
        print_tree(var->expr_list[i], depth +1);
      }
      tab_helper(depth);
      printf(")\n");
    }
    break;
    case Literal: {
      struct Literal *lit = ref.ref;
      tab_helper(depth);
      switch (lit->type) {
        case i32: {
          printf("i32(");
        }
        break;
        case i16:{
          printf("i16(");
        }
        break;
        case i8:{
          printf("i8(");
        }
        break;
      }
      printf("%lld)\n", lit->value);
      }
    break;
    }
  }

void test_kernel_expr() {
  KernelPredicate* pred = get_kernel_expression();
  struct ExpressionRef ref = construct_predicate(pred);
  print_tree(ref, 0);
}
