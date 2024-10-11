#include "expression.h"
int main()
{
  SharedExpression* pred = get_kernel_expression();
  ExpressionRef ref = construct_predicate(pred);
  print_tree(ref, 0);
}
