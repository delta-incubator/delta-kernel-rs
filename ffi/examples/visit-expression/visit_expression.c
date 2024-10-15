#include "delta_kernel_ffi.h"
#include "expression.h"
#include "expression_print.h"

int main() {
  SharedExpression* pred = get_testing_kernel_expression();
  ExpressionItemList list = construct_predicate(pred);
  ExpressionItem ref = list.list[0];
  print_tree(ref, 0);
  free_expression_list(list);
  free_kernel_predicate(pred);
  return 0;
}
