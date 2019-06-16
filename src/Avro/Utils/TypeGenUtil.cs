using Avro.Schemas;
using Avro.Specific;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;

namespace Avro.Utils
{
    internal static class TypeGenUtil
    {
        internal static EnumDeclarationSyntax CreateEnum(string name, IEnumerable<string> symbols, string doc)
        {
            var enumSymbols =
                symbols.Select(
                    r => SyntaxFactory.EnumMemberDeclaration(
                        SyntaxFacts.IsValidIdentifier(r) ? r : $"_{r}"
                    )
                )
            ;

            var enumDeclaration =
                SyntaxFactory.EnumDeclaration(name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddMembers(
                    enumSymbols.ToArray()
                )
            ;

            if (!string.IsNullOrEmpty(doc))
                enumDeclaration =
                    enumDeclaration.WithLeadingTrivia(
                        CreateSummaryToken(doc)
                    )
                ;

            return enumDeclaration;
        }

        internal static ClassDeclarationSyntax CreateRecordClass(string name, int fieldCount, string avro, string doc)
        {
            var classDeclarationSyntax =
                SyntaxFactory.ClassDeclaration(name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SyntaxFactory.SimpleBaseType(
                        SyntaxFactory.ParseTypeName(typeof(ISpecificRecord).FullName)
                    )
                )
                .AddMembers(
                    SyntaxFactory.FieldDeclaration(
                        SyntaxFactory.VariableDeclaration(
                            SyntaxFactory.ParseTypeName(typeof(Schema).FullName)
                        )
                        .AddVariables(
                            SyntaxFactory.VariableDeclarator(
                                SyntaxFactory.Identifier("_SCHEMA")
                            ).WithInitializer(
                                SyntaxFactory.EqualsValueClause(
                                    SyntaxFactory.InvocationExpression(
                                        SyntaxFactory.MemberAccessExpression(
                                            SyntaxKind.SimpleMemberAccessExpression,
                                            SyntaxFactory.MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                                                SyntaxFactory.IdentifierName(typeof(Schema).Name)
                                            ),
                                            SyntaxFactory.IdentifierName(nameof(Schema.Parse))
                                        )
                                    )
                                    .WithArgumentList(
                                        SyntaxFactory.ArgumentList(
                                            SyntaxFactory.SingletonSeparatedList(
                                                SyntaxFactory.Argument(
                                                    SyntaxFactory.LiteralExpression(
                                                        SyntaxKind.StringLiteralExpression,
                                                        SyntaxFactory.Literal(avro)
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                    .AddModifiers(
                        SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                        SyntaxFactory.Token(SyntaxKind.StaticKeyword),
                        SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword)
                    ),
                    SyntaxFactory.PropertyDeclaration(
                        SyntaxFactory.QualifiedName(
                            SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                            SyntaxFactory.IdentifierName(nameof(Schema))
                        ),
                        SyntaxFactory.Identifier("Schema")
                    )
                    .WithModifiers(
                        SyntaxFactory.TokenList(
                            SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        SyntaxFactory.ArrowExpressionClause(
                            SyntaxFactory.IdentifierName("_SCHEMA")
                        )
                    )
                    .WithSemicolonToken(
                        SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                    ),
                    SyntaxFactory.PropertyDeclaration(
                        SyntaxFactory.PredefinedType(
                            SyntaxFactory.Token(SyntaxKind.IntKeyword)
                        ),
                        SyntaxFactory.Identifier("FieldCount")
                    )
                    .WithModifiers(
                        SyntaxFactory.TokenList(
                            SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        SyntaxFactory.ArrowExpressionClause(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(fieldCount)
                            )
                        )
                    )
                    .WithSemicolonToken(
                        SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                    )
                )
            ;

            if (!string.IsNullOrEmpty(doc))
                classDeclarationSyntax =
                    classDeclarationSyntax.WithLeadingTrivia(
                        CreateSummaryToken(doc)
                    )
                ;

            return classDeclarationSyntax;
        }

        internal static ClassDeclarationSyntax CreateErrorClass(string name, int fieldCount, string avro, string doc)
        {
            var classDeclarationSyntax =
                SyntaxFactory.ClassDeclaration(name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SyntaxFactory.SimpleBaseType(
                        SyntaxFactory.ParseTypeName(typeof(AvroRemoteException).FullName)
                    ),
                    SyntaxFactory.SimpleBaseType(
                        SyntaxFactory.ParseTypeName(typeof(ISpecificRecord).FullName)
                    )
                )
                .AddMembers(
                    SyntaxFactory.FieldDeclaration(
                        SyntaxFactory.VariableDeclaration(
                            SyntaxFactory.ParseTypeName(typeof(Schema).FullName)
                        )
                        .AddVariables(
                            SyntaxFactory.VariableDeclarator(
                                SyntaxFactory.Identifier("_SCHEMA")
                            ).WithInitializer(
                                SyntaxFactory.EqualsValueClause(
                                    SyntaxFactory.InvocationExpression(
                                        SyntaxFactory.MemberAccessExpression(
                                            SyntaxKind.SimpleMemberAccessExpression,
                                            SyntaxFactory.MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                                                SyntaxFactory.IdentifierName(typeof(Schema).Name)
                                            ),
                                            SyntaxFactory.IdentifierName(nameof(Schema.Parse))
                                        )
                                    )
                                    .WithArgumentList(
                                        SyntaxFactory.ArgumentList(
                                            SyntaxFactory.SingletonSeparatedList(
                                                SyntaxFactory.Argument(
                                                    SyntaxFactory.LiteralExpression(
                                                        SyntaxKind.StringLiteralExpression,
                                                        SyntaxFactory.Literal(avro)
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                    .AddModifiers(
                        SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                        SyntaxFactory.Token(SyntaxKind.StaticKeyword),
                        SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword)
                    ),
                    SyntaxFactory.PropertyDeclaration(
                        SyntaxFactory.QualifiedName(
                            SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                            SyntaxFactory.IdentifierName(nameof(Schema))
                        ),
                        SyntaxFactory.Identifier("Schema")
                    )
                    .WithModifiers(
                        SyntaxFactory.TokenList(
                            SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        SyntaxFactory.ArrowExpressionClause(
                            SyntaxFactory.IdentifierName("_SCHEMA")
                        )
                    )
                    .WithSemicolonToken(
                        SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                    ),
                    SyntaxFactory.PropertyDeclaration(
                        SyntaxFactory.PredefinedType(
                            SyntaxFactory.Token(SyntaxKind.IntKeyword)
                        ),
                        SyntaxFactory.Identifier("FieldCount")
                    )
                    .WithModifiers(
                        SyntaxFactory.TokenList(
                            SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        SyntaxFactory.ArrowExpressionClause(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(fieldCount)
                            )
                        )
                    )
                    .WithSemicolonToken(
                        SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                    )
                )
            ;

            if (!string.IsNullOrEmpty(doc))
                classDeclarationSyntax =
                    classDeclarationSyntax.WithLeadingTrivia(
                        CreateSummaryToken(doc)
                    )
                ;

            return classDeclarationSyntax;
        }

        internal static ClassDeclarationSyntax CreateFixedClass(string name, string avro, int size)
        {
            var classDeclarationSyntax =
                SyntaxFactory.ClassDeclaration(name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SyntaxFactory.SimpleBaseType(
                        SyntaxFactory.ParseTypeName(typeof(ISpecificFixed).FullName)
                    )
                )
                .WithMembers(
                    SyntaxFactory.List(
                        new MemberDeclarationSyntax[]{
                            SyntaxFactory.FieldDeclaration(
                                SyntaxFactory.VariableDeclaration(
                                    SyntaxFactory.QualifiedName(
                                        SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                                        SyntaxFactory.IdentifierName(nameof(Schema))
                                    )
                                )
                                .WithVariables(
                                    SyntaxFactory.SingletonSeparatedList(
                                        SyntaxFactory.VariableDeclarator(
                                            SyntaxFactory.Identifier("_SCHEMA")
                                        )
                                        .WithInitializer(
                                            SyntaxFactory.EqualsValueClause(
                                                SyntaxFactory.InvocationExpression(
                                                    SyntaxFactory.MemberAccessExpression(
                                                        SyntaxKind.SimpleMemberAccessExpression,
                                                        SyntaxFactory.MemberAccessExpression(
                                                            SyntaxKind.SimpleMemberAccessExpression,
                                                            SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                                                            SyntaxFactory.IdentifierName(typeof(Schema).Name)
                                                        ),
                                                        SyntaxFactory.IdentifierName(nameof(Schema.Parse))
                                                    )
                                                )
                                                .WithArgumentList(
                                                    SyntaxFactory.ArgumentList(
                                                        SyntaxFactory.SingletonSeparatedList(
                                                            SyntaxFactory.Argument(
                                                                SyntaxFactory.LiteralExpression(
                                                                    SyntaxKind.StringLiteralExpression,
                                                                    SyntaxFactory.Literal(avro)
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            .WithModifiers(
                                SyntaxFactory.TokenList(
                                    new []{
                                        SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                                        SyntaxFactory.Token(SyntaxKind.StaticKeyword),
                                        SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword)
                                    }
                                )
                            ),
                            SyntaxFactory.FieldDeclaration(
                                SyntaxFactory.VariableDeclaration(
                                    SyntaxFactory.PredefinedType(
                                        SyntaxFactory.Token(SyntaxKind.IntKeyword)
                                    )
                                )
                                .WithVariables(
                                    SyntaxFactory.SingletonSeparatedList(
                                        SyntaxFactory.VariableDeclarator(
                                            SyntaxFactory.Identifier("_SIZE")
                                        )
                                        .WithInitializer(
                                            SyntaxFactory.EqualsValueClause(
                                                SyntaxFactory.LiteralExpression(
                                                    SyntaxKind.NumericLiteralExpression,
                                                    SyntaxFactory.Literal(size)
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            .WithModifiers(
                                SyntaxFactory.TokenList(
                                    new []{
                                        SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                                        SyntaxFactory.Token(SyntaxKind.ConstKeyword)
                                    }
                                )
                            ),
                            SyntaxFactory.ConstructorDeclaration(
                                SyntaxFactory.Identifier(name)
                            )
                            .WithModifiers(
                                SyntaxFactory.TokenList(
                                    SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithBody(
                                SyntaxFactory.Block(
                                    SyntaxFactory.ExpressionStatement(
                                        SyntaxFactory.AssignmentExpression(
                                            SyntaxKind.SimpleAssignmentExpression,
                                            SyntaxFactory.IdentifierName("Value"),
                                            SyntaxFactory.ArrayCreationExpression(
                                                SyntaxFactory.ArrayType(
                                                    SyntaxFactory.PredefinedType(
                                                        SyntaxFactory.Token(SyntaxKind.ByteKeyword)
                                                    )
                                                )
                                                .WithRankSpecifiers(
                                                    SyntaxFactory.SingletonList(
                                                        SyntaxFactory.ArrayRankSpecifier(
                                                            SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                                                SyntaxFactory.IdentifierName("_SIZE")
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            ),
                            SyntaxFactory.PropertyDeclaration(
                                SyntaxFactory.QualifiedName(
                                    SyntaxFactory.IdentifierName(typeof(Schema).Namespace),
                                    SyntaxFactory.IdentifierName(nameof(Schema))
                                ),
                                SyntaxFactory.Identifier("Schema")
                            )
                            .WithModifiers(
                                SyntaxFactory.TokenList(
                                    SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithExpressionBody(
                                SyntaxFactory.ArrowExpressionClause(
                                    SyntaxFactory.IdentifierName("_SCHEMA")
                                )
                            )
                            .WithSemicolonToken(
                                SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                            ),
                            SyntaxFactory.PropertyDeclaration(
                                SyntaxFactory.PredefinedType(
                                    SyntaxFactory.Token(SyntaxKind.IntKeyword)
                                ),
                                SyntaxFactory.Identifier("FixedSize")
                            )
                            .WithModifiers(
                                SyntaxFactory.TokenList(
                                    SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithExpressionBody(
                                SyntaxFactory.ArrowExpressionClause(
                                    SyntaxFactory.IdentifierName("_SIZE")
                                )
                            )
                            .WithSemicolonToken(
                                SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                            ),
                            SyntaxFactory.PropertyDeclaration(
                                SyntaxFactory.ArrayType(
                                    SyntaxFactory.PredefinedType(
                                        SyntaxFactory.Token(SyntaxKind.ByteKeyword)
                                    )
                                )
                                .WithRankSpecifiers(
                                    SyntaxFactory.SingletonList(
                                        SyntaxFactory.ArrayRankSpecifier(
                                            SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                                SyntaxFactory.OmittedArraySizeExpression()
                                            )
                                        )
                                    )
                                ),
                                SyntaxFactory.Identifier("Value")
                            )
                            .WithModifiers(
                                SyntaxFactory.TokenList(
                                    SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithAccessorList(
                                SyntaxFactory.AccessorList(
                                    SyntaxFactory.List(
                                        new AccessorDeclarationSyntax[]{
                                            SyntaxFactory.AccessorDeclaration(
                                                SyntaxKind.GetAccessorDeclaration
                                            )
                                            .WithSemicolonToken(
                                                SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                                            ),
                                            SyntaxFactory.AccessorDeclaration(
                                                SyntaxKind.SetAccessorDeclaration
                                            )
                                            .WithModifiers(
                                                SyntaxFactory.TokenList(
                                                    SyntaxFactory.Token(SyntaxKind.PrivateKeyword)
                                                )
                                            )
                                            .WithSemicolonToken(
                                                SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                                            )
                                        }
                                    )
                                )
                            )
                        }
                    )
                )
            ;

            return classDeclarationSyntax;
        }

        internal static PropertyDeclarationSyntax CreateClassProperty(string name, string type, string doc)
        {
            var propertyDeclaration =
                SyntaxFactory.PropertyDeclaration(
                    SyntaxFactory.ParseTypeName(type),
                    name
                )
                .AddModifiers(
                    SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                )
                .WithAccessorList(
                    SyntaxFactory.AccessorList()
                        .AddAccessors(
                            SyntaxFactory.AccessorDeclaration(
                                SyntaxKind.GetAccessorDeclaration
                            )
                            .WithSemicolonToken(
                                SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                            ),
                            SyntaxFactory.AccessorDeclaration(
                                SyntaxKind.SetAccessorDeclaration
                            )
                            .WithSemicolonToken(
                                SyntaxFactory.Token(SyntaxKind.SemicolonToken)
                            )
                        )
                )
            ;

            if (!string.IsNullOrEmpty(doc))
                propertyDeclaration =
                    propertyDeclaration.WithLeadingTrivia(
                        CreateSummaryToken(doc)
                    )
                ;

            return propertyDeclaration;
        }

        internal static ClassDeclarationSyntax AddPropertiesToClass(ClassDeclarationSyntax classDeclarationSyntax, IEnumerable<PropertyDeclarationSyntax> propertyDeclarationSyntaxes)
        {
            return classDeclarationSyntax
                .AddMembers(
                    propertyDeclarationSyntaxes.ToArray()
                )
            ;
        }

        internal static ClassDeclarationSyntax AddMembersToClass(ClassDeclarationSyntax classDeclarationSyntax, params MemberDeclarationSyntax[] memberDeclarationSyntax)
        {
            return classDeclarationSyntax
                .AddMembers(
                    memberDeclarationSyntax
                )
            ;
        }

        internal static MemberDeclarationSyntax CreateClassGetMethod(IEnumerable<SwitchSectionSyntax> getSwitchSectionSyntaxes, int maxRange)
        {
            return
                SyntaxFactory.MethodDeclaration(
                    SyntaxFactory.PredefinedType(
                        SyntaxFactory.Token(SyntaxKind.ObjectKeyword)
                    ),
                    SyntaxFactory.Identifier("Get")
                )
                .WithModifiers(
                    SyntaxFactory.TokenList(
                        SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                    )
                )
                .WithParameterList(
                    SyntaxFactory.ParameterList(
                        SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.Parameter(
                                SyntaxFactory.Identifier("fieldPos")
                            )
                            .WithType(
                                SyntaxFactory.PredefinedType(
                                    SyntaxFactory.Token(SyntaxKind.IntKeyword)
                                )
                            )
                        )
                    )
                )
                .WithBody(
                    SyntaxFactory.Block(
                        SyntaxFactory.SwitchStatement(
                            SyntaxFactory.IdentifierName("fieldPos")
                        )
                        .WithOpenParenToken(
                            SyntaxFactory.Token(SyntaxKind.OpenParenToken)
                        )
                        .WithCloseParenToken(
                            SyntaxFactory.Token(SyntaxKind.CloseParenToken)
                        )
                        .WithSections(
                            SyntaxFactory.List(
                                getSwitchSectionSyntaxes.Append(SwitchCaseDefaultIndexOutOfRange(maxRange))
                            )
                        )
                    )
                )
            ;
        }

        internal static MemberDeclarationSyntax CreateClassSetMethod(IEnumerable<SwitchSectionSyntax> switchSectionSyntaxes, int maxRange)
        {
            return
                SyntaxFactory.MethodDeclaration(
                    SyntaxFactory.PredefinedType(
                        SyntaxFactory.Token(SyntaxKind.VoidKeyword)
                    ),
                    SyntaxFactory.Identifier("Put")
                )
                .WithModifiers(
                    SyntaxFactory.TokenList(
                        SyntaxFactory.Token(SyntaxKind.PublicKeyword)
                    )
                )
                .WithParameterList(
                    SyntaxFactory.ParameterList(
                        SyntaxFactory.SeparatedList<ParameterSyntax>(
                            new SyntaxNodeOrToken[]{
                                SyntaxFactory.Parameter(
                                    SyntaxFactory.Identifier("fieldPos")
                                )
                                .WithType(
                                    SyntaxFactory.PredefinedType(
                                        SyntaxFactory.Token(SyntaxKind.IntKeyword)
                                    )
                                ),
                                SyntaxFactory.Token(SyntaxKind.CommaToken),
                                SyntaxFactory.Parameter(
                                    SyntaxFactory.Identifier("fieldValue")
                                )
                                .WithType(
                                    SyntaxFactory.PredefinedType(
                                        SyntaxFactory.Token(SyntaxKind.ObjectKeyword)
                                    )
                                )
                            }
                        )
                    )
                )
                .WithBody(
                    SyntaxFactory.Block(
                        SyntaxFactory.SwitchStatement(
                            SyntaxFactory.IdentifierName("fieldPos")
                        )
                        .WithOpenParenToken(
                            SyntaxFactory.Token(SyntaxKind.OpenParenToken)
                        )
                        .WithCloseParenToken(
                            SyntaxFactory.Token(SyntaxKind.CloseParenToken)
                        )
                        .WithSections(
                            SyntaxFactory.List(
                                switchSectionSyntaxes.Append(SwitchCaseDefaultIndexOutOfRange(maxRange))
                            )
                        )
                    )
                )
            ;
        }

        internal static SwitchSectionSyntax SwitchCaseGetProperty(string caseLabel, string propertyName)
        {
            return
                SyntaxFactory.SwitchSection()
                .WithLabels(
                    SyntaxFactory.SingletonList<SwitchLabelSyntax>(
                        SyntaxFactory.CaseSwitchLabel(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(caseLabel)
                            )
                        )
                    )
                )
                .WithStatements(
                    SyntaxFactory.SingletonList<StatementSyntax>(
                        SyntaxFactory.ReturnStatement(
                            SyntaxFactory.MemberAccessExpression(
                                SyntaxKind.SimpleMemberAccessExpression,
                                SyntaxFactory.ThisExpression(),
                                SyntaxFactory.IdentifierName(propertyName)
                            )
                        )
                    )
                )
            ;
        }

        internal static SwitchSectionSyntax SwitchCaseSetProperty(string caseLabel, string propertyName, string propertyType)
        {
            return
                SyntaxFactory.SwitchSection()
                .WithLabels(
                    SyntaxFactory.SingletonList<SwitchLabelSyntax>(
                        SyntaxFactory.CaseSwitchLabel(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(caseLabel)
                            )
                        )
                    )
                )
                .WithStatements(
                    SyntaxFactory.List(
                        new StatementSyntax[]{
                            SyntaxFactory.ExpressionStatement(
                                SyntaxFactory.AssignmentExpression(
                                    SyntaxKind.SimpleAssignmentExpression,
                                    SyntaxFactory.MemberAccessExpression(
                                        SyntaxKind.SimpleMemberAccessExpression,
                                        SyntaxFactory.ThisExpression(),
                                        SyntaxFactory.IdentifierName(propertyName)
                                    ),
                                    SyntaxFactory.CastExpression(
                                        SyntaxFactory.ParseTypeName(propertyType),
                                        SyntaxFactory.IdentifierName("value")
                                    )
                                )
                            ),
                            SyntaxFactory.BreakStatement()
                        }
                    )
                )
            ;
        }

        private static SwitchSectionSyntax SwitchCaseDefaultIndexOutOfRange(int maxRange)
        {
            return
                SyntaxFactory.SwitchSection()
                .WithLabels(
                    SyntaxFactory.SingletonList<SwitchLabelSyntax>(
                        SyntaxFactory.DefaultSwitchLabel()
                    )
                )
                .WithStatements(
                    SyntaxFactory.SingletonList<StatementSyntax>(
                        SyntaxFactory.ThrowStatement(
                            SyntaxFactory.ObjectCreationExpression(
                                SyntaxFactory.IdentifierName("IndexOutOfRangeException")
                            )
                            .WithArgumentList(
                                SyntaxFactory.ArgumentList(
                                    SyntaxFactory.SingletonSeparatedList(
                                        SyntaxFactory.Argument(
                                            SyntaxFactory.LiteralExpression(
                                                SyntaxKind.StringLiteralExpression,
                                                SyntaxFactory.Literal($"Expected range: [0:{maxRange}].")
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ;
        }

        internal static string ToString(SyntaxNode syntaxNode)
        {
            return
                syntaxNode
                .NormalizeWhitespace()
                .ToFullString()
            ;
        }

        internal static MemberDeclarationSyntax QualifyMember(MemberDeclarationSyntax memberDeclarationSyntax, string ns)
        {
            if (!string.IsNullOrEmpty(ns))
                return SyntaxFactory.NamespaceDeclaration(SyntaxFactory.ParseName(ns))
                    .AddMembers(memberDeclarationSyntax);
            else
                return memberDeclarationSyntax;
        }

        private static SyntaxTriviaList CreateSummaryToken(string doc)
        {
            return
                SyntaxFactory.TriviaList(
                        SyntaxFactory.Trivia(
                            SyntaxFactory.DocumentationCommentTrivia(
                                SyntaxKind.SingleLineDocumentationCommentTrivia,
                                SyntaxFactory.List<XmlNodeSyntax>(
                                    new XmlNodeSyntax[]{
                                        SyntaxFactory.XmlText()
                                        .WithTextTokens(
                                            SyntaxFactory.TokenList(
                                                SyntaxFactory.XmlTextLiteral(
                                                    SyntaxFactory.TriviaList(
                                                        SyntaxFactory.DocumentationCommentExterior("///")
                                                    ),
                                                    " ",
                                                    " ",
                                                    SyntaxFactory.TriviaList()
                                                )
                                            )
                                        ),
                                        SyntaxFactory.XmlExampleElement(
                                            SyntaxFactory.SingletonList<XmlNodeSyntax>(
                                                SyntaxFactory.XmlText()
                                                .WithTextTokens(
                                                    SyntaxFactory.TokenList(
                                                        new []{
                                                            SyntaxFactory.XmlTextNewLine(
                                                                SyntaxFactory.TriviaList(),
                                                                "\n",
                                                                "\n",
                                                                SyntaxFactory.TriviaList()
                                                            ),
                                                            SyntaxFactory.XmlTextLiteral(
                                                                SyntaxFactory.TriviaList(
                                                                    SyntaxFactory.DocumentationCommentExterior("///")
                                                                ),
                                                                $" {doc}",
                                                                $" {doc}",
                                                                SyntaxFactory.TriviaList()
                                                            ),
                                                            SyntaxFactory.XmlTextNewLine(
                                                                SyntaxFactory.TriviaList(),
                                                                "\n",
                                                                "\n",
                                                                SyntaxFactory.TriviaList()
                                                            ),
                                                            SyntaxFactory.XmlTextLiteral(
                                                                SyntaxFactory.TriviaList(
                                                                    SyntaxFactory.DocumentationCommentExterior("///")
                                                                ),
                                                                " ",
                                                                " ",
                                                                SyntaxFactory.TriviaList()
                                                            )
                                                        }
                                                    )
                                                )
                                            )
                                        )
                                        .WithStartTag(
                                            SyntaxFactory.XmlElementStartTag(
                                                SyntaxFactory.XmlName(
                                                    SyntaxFactory.Identifier("summary")
                                                )
                                            )
                                        )
                                        .WithEndTag(
                                            SyntaxFactory.XmlElementEndTag(
                                                SyntaxFactory.XmlName(
                                                    SyntaxFactory.Identifier("summary")
                                                )
                                            )
                                        ),
                                        SyntaxFactory.XmlText()
                                        .WithTextTokens(
                                            SyntaxFactory.TokenList(
                                                SyntaxFactory.XmlTextNewLine(
                                                    SyntaxFactory.TriviaList(),
                                                    "\n",
                                                    "\n",
                                                    SyntaxFactory.TriviaList()
                                                )
                                            )
                                        )
                                    }
                                )
                            )
                        )
                    )
                ;
        }

        internal static CompilationUnitSyntax CreateCompileUnit(MemberDeclarationSyntax memberDeclarationSyntax)
        {
            return
                SyntaxFactory.CompilationUnit()
                .WithUsings(
                    SyntaxFactory.List(
                        new UsingDirectiveSyntax[]{
                            SyntaxFactory.UsingDirective(
                                SyntaxFactory.IdentifierName("System")
                            )
                        }
                    )
                )
                .WithMembers(
                    SyntaxFactory.SingletonList(
                        memberDeclarationSyntax
                    )
                );
        }

        internal static CompilationUnitSyntax CreateCompileUnit(IEnumerable<MemberDeclarationSyntax> memberDeclarationSyntaxes)
        {
            return
                SyntaxFactory.CompilationUnit()
                .WithUsings(
                    SyntaxFactory.List(
                        new UsingDirectiveSyntax[]{
                            SyntaxFactory.UsingDirective(
                                SyntaxFactory.IdentifierName("System")
                            )
                        }
                    )
                )
                .WithMembers(
                    SyntaxFactory.List(
                        memberDeclarationSyntaxes
                    )
                );
        }

        internal static Assembly Compile(string assemblyName, string code, out XmlDocument xmlDocumentation)
        {
            var references = new[] {
                MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof(ISpecificRecord).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            };

            var compilationUnit = SyntaxFactory.ParseCompilationUnit(code);

            var cSharpCompilation = CSharpCompilation.Create(
                assemblyName,
                new[] { compilationUnit.SyntaxTree },
                references,
                new CSharpCompilationOptions(
                    OutputKind.DynamicallyLinkedLibrary
                )
            );

            using (var ms = new MemoryStream())
            using (var ds = new MemoryStream())
            {
                var result = cSharpCompilation.Emit(
                    peStream: ms,
                    xmlDocumentationStream: ds
                );

                if (!result.Success)
                {
                    var compilationErrors =
                        result.Diagnostics.Where(
                            diagnostic =>
                            diagnostic.IsWarningAsError ||
                            diagnostic.Severity == DiagnosticSeverity.Error
                        )
                        .ToList();
                    if (compilationErrors.Any())
                    {
                        var firstError = compilationErrors.First();
                        var errorNumber = firstError.Id;
                        var errorDescription = firstError.GetMessage();
                        var firstErrorMessage = $"{errorNumber}: {errorDescription};";
                        throw new Exception($"Compilation failed, first error is: {firstErrorMessage}");
                    }
                }

                ds.Seek(0, SeekOrigin.Begin);
                xmlDocumentation = new XmlDocument();
                xmlDocumentation.Load(ds);

                var assembly = Assembly.Load(ms.ToArray());
                return assembly;
            }
        }

        public static string GetSystemType(Schema schema, bool nullable = false)
        {
            switch (schema.GetType().Name)
            {
                case nameof(NullSchema):
                    return typeof(object).FullName;
                case nameof(BooleanSchema):
                    if (nullable)
                        return typeof(bool?).FullName;
                    else
                        return typeof(bool).FullName;
                case nameof(IntSchema):
                    if (nullable)
                        return typeof(int?).FullName;
                    else
                        return typeof(int).FullName;
                case nameof(LongSchema):
                    if (nullable)
                        return typeof(long).FullName;
                    else
                        return typeof(long).FullName;
                case nameof(FloatSchema):
                    if (nullable)
                        return typeof(float?).FullName;
                    else
                        return typeof(float).FullName;
                case nameof(DoubleSchema):
                    if (nullable)
                        return typeof(double).FullName;
                    else
                        return typeof(double).FullName;
                case nameof(BytesSchema):
                    return typeof(byte[]).FullName;
                case nameof(StringSchema):
                    return typeof(string).FullName;

                case nameof(ArraySchema):
                    return GetSystemType(schema as ArraySchema);
                case nameof(MapSchema):
                    return GetSystemType(schema as MapSchema);

                case nameof(FixedSchema):
                case nameof(EnumSchema):
                case nameof(RecordSchema):
                case nameof(ErrorSchema):
                    return GetSystemType(schema as NamedSchema);

                case nameof(UnionSchema):
                    return GetSystemType(schema as UnionSchema);

                default:
                    return typeof(object).FullName;
            }
        }

        private static string GetSystemType(ArraySchema schema)
        {
            return $"IList<{GetSystemType(schema.Items)}>";
        }

        private static string GetSystemType(MapSchema schema)
        {
            return $"IDictionary<{typeof(string).FullName}, {GetSystemType((schema as MapSchema).Values)}>";
        }

        private static string GetSystemType(NamedSchema schema)
        {
            var name = schema.Name;
            if (string.IsNullOrEmpty(schema.Namespace))
                return schema.Name;
            else
                return $"{schema.Namespace}.{schema.Name}";
        }

        private static string GetSystemType(UnionSchema schema)
        {
            switch (schema.Count())
            {
                case 1:
                    return GetSystemType(schema.First());
                case 2:
                    if (schema.FirstOrDefault(r => r.GetType().Name == nameof(NullSchema)) != null)
                        return GetSystemType(schema.First(), true);
                    break;
            }
            return typeof(object).FullName;
        }
    }
}
