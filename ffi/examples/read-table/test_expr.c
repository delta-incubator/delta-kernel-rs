#include "expression.h"
int main()
{
  SharedExpression* pred = get_kernel_expression();
  ExpressionItemList list = construct_predicate(pred);
  ExpressionItem ref = list.exprList[0];
  print_tree(ref, 0);
}
