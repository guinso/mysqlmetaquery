package mysqlmetaquery

import (
	"errors"
	"fmt"
	"strings"

	"github.com/guinso/rdbmstool"
	"github.com/guinso/rdbmstool/parser"
)

//GetViewNames show all database's view table
//db : sql.DB or sql.Tx or compatible with it
//dbName: database schema name
//searchPattern: view name patterm (can use % as wildcard; example - 'view_%')
func (meta *MySQLMetaQuery) GetViewNames(
	db rdbmstool.DbHandlerProxy,
	dbName string,
	searchPattern string) ([]string, error) {

	rows, queryErr := db.Query("SELECT table_name FROM information_schema.views"+
		" WHERE table_schema = ? AND table_name LIKE ?", dbName, searchPattern)

	if queryErr != nil {
		return nil, queryErr
	}

	result := make([]string, 5)
	for rows.Next() {
		tmp := ""
		rows.Scan(&tmp)
		result = append(result, tmp)
	}

	return result, nil
}

//GetViewDefinition get data view definition
//db : sql.DB or sql.Tx or compatible with it
//dbName: database name
//viewName: view name (example 'tax_invoice')
func (meta *MySQLMetaQuery) GetViewDefinition(db rdbmstool.DbHandlerProxy, dbName string, viewName string) (
	*rdbmstool.ViewDefinition, error) {

	//get raw SQL statement from database schema
	rawDef, rawErr := getViewDefinition(db, dbName, viewName)
	if rawErr != nil {
		return nil, rawErr
	}

	//tokenize, parsing into abstract syntax tree
	ast, astErr := parser.ParseSQL(rawDef)
	if astErr != nil {
		return nil, astErr
	}

	if ast.DataType != parser.NodeQuery {
		return nil, fmt.Errorf("Parsed syntax tree is not query AST")
	}

	//TODO: build ViewDefinition based on AST
	viewDef := rdbmstool.NewViewDefinition("")
	for _, node := range ast.ChildNodes {
		switch node.DataType {
		case parser.NodeSelect:
			for _, col := range node.ChildNodes {
				if len(col.ChildNodes) == 2 &&
					col.ChildNodes[0].DataType == parser.NodeColName &&
					col.ChildNodes[1].DataType == parser.NodeAlias {
					viewDef.Query.Select(col.ChildNodes[0].RawString(), col.ChildNodes[1].RawString())
				} else if len(col.ChildNodes) == 1 &&
					col.ChildNodes[0].DataType == parser.NodeColName {
					viewDef.Query.Select(col.ChildNodes[0].RawString(), "")
				} else {
					return nil, fmt.Errorf("Invalid AST for SELECT syntax")
				}
			}
			break
		case parser.NodeFrom:
			if len(node.ChildNodes) == 1 &&
				node.ChildNodes[0].DataType == parser.NodeSource {
				viewDef.Query.From(node.ChildNodes[0].RawString(), "")
			} else if len(node.ChildNodes) == 2 &&
				node.ChildNodes[0].DataType == parser.NodeSource &&
				node.ChildNodes[1].DataType == parser.NodeAlias {
				viewDef.Query.From(
					node.ChildNodes[0].RawString(),
					node.ChildNodes[1].RawString())
			} else {
				return nil, fmt.Errorf("Invalid AST for FROM syntax")
			}
			break
		case parser.NodeJoin:
			if err := convertJoinAST(viewDef, &node); err != nil {
				return nil, err
			}
			break
		case parser.NodeWhere:
			//TODO: implement
			break
		}
	}

	return nil, errors.New("Not implemented yet")
}

func convertWhereAST(viewDef *rdbmstool.ViewDefinition, ast *parser.SyntaxTree) error {
	if len(ast.ChildNodes) != 1 {
		return fmt.Errorf("No node found in WHERE syntax tree")
	}

	if ast.ChildNodes[0].DataType != parser.NodeCondition {
		return fmt.Errorf("No condition node found in WHERE syntax tree")
	}

	//convert condition...

	return nil
}

func convertCondition(ast *parser.SyntaxTree) (*rdbmstool.ConditionDefinition, error) {
	if ast.DataType != parser.NodeCondition {
		return nil, fmt.Errorf(
			"expect syntax tree is condition node but it is not (%T)", ast.DataType)
	}

	nodeLen := len(ast.ChildNodes)

	if nodeLen < 1 {
		return nil, fmt.Errorf("no definition found in condition syntax tree")
	}

	cond := rdbmstool.ConditionDefinition{}

	switch ast.ChildNodes[0].DataType {
	case parser.NodeCondition:
		tmpCond, tmpErr := convertCondition(&ast.ChildNodes[0])
		if tmpErr != nil {
			return nil, tmpErr
		}

		cond.SetComplex(tmpCond)
		break
	case parser.NodeExpression:
		tmpExpr, tmpErr := convertExpression(&ast.ChildNodes[0])
		if tmpErr != nil {
			return nil, tmpErr
		}

		cond.SetCondition(tmpExpr)
		break
	default:
		return nil, fmt.Errorf("Condition ast's 1st node must be expression or sub condition")
	}

	index := 1
	for index < len(ast.ChildNodes) {
		if ast.ChildNodes[index].DataType != parser.NodeOperator {
			return nil, fmt.Errorf("expect operator node but get <%T> at condition ast index %d",
				ast.ChildNodes[index].DataType,
				index)
		}

		token := ast.ChildNodes[index].Source[ast.ChildNodes[index].StartPosition]
		if token.Type != parser.TokenAnd && token.Type != parser.TokenOr {
			return nil, fmt.Errorf("node operator must is token AND / token OR but get <%T>", token.Type)
		}

		if len(ast.ChildNodes) <= index+1 {
			return nil, fmt.Errorf(
				"expect after condition operator must have operand but no more nodes")
		}

		if ast.ChildNodes[index+1].DataType == parser.NodeExpression {
			tmpExpr, tmpExprErr := convertExpression(&ast.ChildNodes[index+1])
			if tmpExprErr != nil {
				return nil, tmpExprErr
			}

			if token.Type == parser.TokenOr {
				cond.AddOr(tmpExpr)
			} else {
				cond.AddAnd(tmpExpr)
			}

			index += 2
			continue
		} else if ast.ChildNodes[index+1].DataType == parser.NodeCondition {
			tmpCond, tmpCondErr := convertCondition(&ast.ChildNodes[index+1])
			if tmpCondErr != nil {
				return nil, tmpCondErr
			}

			if token.Type == parser.TokenOr {
				cond.AddOrComplex(tmpCond)
			} else {
				cond.AddAndComplex(tmpCond)
			}

			index += 2
			continue
		} else {
			return nil, fmt.Errorf(
				"expect node condition or node expression but get <%T> instead",
				ast.ChildNodes[index+1].DataType)
		}
	}

	return &cond, nil
}

func convertExpression(ast *parser.SyntaxTree) (string, error) {
	if ast.DataType != parser.NodeExpression {
		return "", fmt.Errorf("ast is not an expression node")
	}

	return ast.RawString(), nil
}

func convertJoinAST(viewDef *rdbmstool.ViewDefinition, ast *parser.SyntaxTree) error {
	source := ""
	alias := ""
	nodeLen := len(ast.ChildNodes)
	index := 0
	lhs := ""
	rhs := ""
	var join rdbmstool.JoinType
	//var operator rdbmstool.ConditionOperator

	join, joinErr := getJoinType(ast.Source[ast.StartPosition].Type)
	if joinErr != nil {
		return joinErr
	}

	//source
	if nodeLen > index && ast.ChildNodes[index].DataType == parser.NodeSource {
		// if len(ast.ChildNodes[index].ChildNodes) > 0 &&
		// 	ast.ChildNodes[index].ChildNodes[0].DataType == parser.NodeQuerySelect {
		//TODOL
		// } else {
		// 	source = ast.ChildNodes[index].RawString()
		// }

		source = ast.ChildNodes[index].RawString()
		index++
	} else {
		return fmt.Errorf("JOIN ast has no source node (%s)", ast.RawString())
	}

	//alias
	if nodeLen > index && ast.ChildNodes[index].DataType == parser.NodeAlias {
		alias = ast.ChildNodes[index].RawString()
		index++
	}

	//condition
	var operator string
	if nodeLen > index {
		//left hand side
		if ast.ChildNodes[index].DataType == parser.NodeCondition {
			lhs = ast.ChildNodes[index].RawString()
			index++
		} else if ast.ChildNodes[index].DataType == parser.NodeExpression {
			lhs = ast.ChildNodes[index].RawString()
			index++
		} else {
			return fmt.Errorf("Cannot find operand for JOIN condition syntax tree")
		}

		//operator
		if nodeLen > index && ast.ChildNodes[index].DataType == parser.NodeOperator {
			token := ast.ChildNodes[index].Source[ast.ChildNodes[index].StartPosition]
			//operator123, operatorErr := getConditionOperator(token.Type)
			//operator = operator123
			operator = token.String()
			index++

			// if operatorErr != nil {
			// 	return operatorErr
			// }
		} else {
			return fmt.Errorf("Cannot find operator for JOIN condition syntax tree")
		}

		//right hand side
		if ast.ChildNodes[index].DataType == parser.NodeCondition {
			rhs = ast.ChildNodes[index].RawString()
			index++
		} else if ast.ChildNodes[index].DataType == parser.NodeExpression {
			rhs = ast.ChildNodes[index].RawString()
			index++
		} else {
			return fmt.Errorf("Cannot find operand for JOIN condition syntax tree")
		}
	}

	viewDef.Query.JoinAdd(source, alias, join, lhs+" "+operator+" "+rhs)

	return nil
}

func getJoinType(token parser.TokenType) (rdbmstool.JoinType, error) {
	switch token {
	case parser.TokenJoin:
		return rdbmstool.Join, nil
	case parser.TokenLeftJoin:
		return rdbmstool.LeftJoin, nil
	case parser.TokenRightJoin:
		return rdbmstool.RightJoin, nil
	case parser.TokenInnerJoin:
		return rdbmstool.InnerJoin, nil
	case parser.TokenOuterJoin:
		return rdbmstool.OuterJoin, nil
	default:
		return rdbmstool.Join, fmt.Errorf(
			"unsupported token found for JOIN type (%s)",
			token.String())
	}
}

// func getConditionOperator(token parser.TokenType) (rdbmstool.ConditionOperator, error) {
// 	switch token {
// 	case parser.TokenEqual:
// 		return rdbmstool.EQUAL, nil
// 	case parser.TokenNotEqual:
// 		return rdbmstool.NOT_EQUAL, nil
// 	case parser.TokenGreater:
// 		return rdbmstool.GREATER_THAN, nil
// 	case parser.TokenGreaterEqual:
// 		return rdbmstool.GREATER_THAN_EQUAL, nil
// 	case parser.TokenLesser:
// 		return rdbmstool.LESS_THAN, nil
// 	case parser.TokenLesserEqual:
// 		return rdbmstool.LESS_THAN_EQUAL, nil
// 	case parser.TokenBetween:
// 		return rdbmstool.BETWEEN, nil
// 	case parser.TokenLike:
// 		return rdbmstool.LIKE, nil
// 	case parser.TokenIn:
// 		return rdbmstool.IN, nil
// 	default:
// 		return rdbmstool.EQUAL,
// 			fmt.Errorf("unsupported token found for JOIN condition operator (%s)",
// 				token.String())
// 	}
// }

func getViewDefinition(db rdbmstool.DbHandlerProxy, dbName string, viewName string) (string, error) {
	rows, err := db.Query("SELECT VIEW_DEFINITION "+
		"FROM information_schema.views "+
		"WHERE table_schema=? AND table_name=?", dbName, viewName)

	if err != nil {
		return "", err
	}

	rawDef := ""
	if rows.Next() {
		err := rows.Scan(&rawDef)

		if err != nil {
			rows.Close()
			return "", err
		}
	}
	rows.Close()

	if strings.Compare(rawDef, "") == 0 {
		return "", fmt.Errorf(
			"No view definition found for %s.%s",
			dbName,
			viewName)
	}

	return rawDef, nil
}
