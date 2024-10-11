#include "expression.h"
int main()
{
  SharedExpression* pred = get_kernel_expression();
  ExpressionItem ref = construct_predicate(pred);
  print_tree(ref, 0);
}
