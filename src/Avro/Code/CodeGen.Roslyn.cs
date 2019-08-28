using Avro.Protocol;
using Avro.Schema;
using Avro.Types;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Avro.Code
{
    public partial class CodeGen
    {
        internal static EnumDeclarationSyntax CreateEnum(string name, IEnumerable<string> symbols, string doc, IEnumerable<string> aliases)
        {
            var enumSymbols =
                symbols.Select(
                    r => EnumMemberDeclaration(
                        SyntaxFacts.IsReservedKeyword(
                            SyntaxFacts.GetKeywordKind(r)
                         ) ?
                         $"_{r}" :
                         r
                    )
                )
            ;

            var enumDeclaration =
                EnumDeclaration(name)
                .AddModifiers(Token(SyntaxKind.PublicKeyword))
                .AddMembers(
                    enumSymbols.ToArray()
                )
            ;

            enumDeclaration =
                enumDeclaration.WithLeadingTrivia(
                    CreateSummaryToken(doc, aliases.ToArray())
                );

            return enumDeclaration;
        }

        internal static ClassDeclarationSyntax CreateRecordClass(string name, int fieldCount, string avro, string doc, IEnumerable<string> aliases)
        {
            var classDeclarationSyntax =
                ClassDeclaration(name)
                .AddModifiers(Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SimpleBaseType(
                        ParseTypeName(typeof(IAvroRecord).FullName)
                    )
                )
                .AddMembers(
                    FieldDeclaration(
                        VariableDeclaration(
                            IdentifierName(nameof(RecordSchema))
                        )
                        .WithVariables(
                            SingletonSeparatedList(
                                VariableDeclarator(
                                    Identifier("_SCHEMA")
                                )
                                .WithInitializer(
                                    EqualsValueClause(
                                        InvocationExpression(
                                            MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                IdentifierName(nameof(AvroParser)),
                                                GenericName(
                                                    Identifier(nameof(AvroParser.ReadSchema))
                                                )
                                                .WithTypeArgumentList(
                                                    TypeArgumentList(
                                                        SingletonSeparatedList<TypeSyntax>(
                                                            IdentifierName(nameof(RecordSchema))
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                        .WithArgumentList(
                                            ArgumentList(
                                                SingletonSeparatedList(
                                                    Argument(
                                                        LiteralExpression(
                                                            SyntaxKind.StringLiteralExpression,
                                                            Literal(avro)
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
                        TokenList(
                            new[]{
                                Token(SyntaxKind.PublicKeyword),
                                Token(SyntaxKind.StaticKeyword),
                                Token(SyntaxKind.ReadOnlyKeyword)
                            }
                        )
                    ),
                    PropertyDeclaration(
                        IdentifierName(nameof(RecordSchema)),
                        Identifier("Schema")
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        ArrowExpressionClause(
                            IdentifierName("_SCHEMA")
                        )
                    )
                    .WithSemicolonToken(
                        Token(SyntaxKind.SemicolonToken)
                    ),
                    PropertyDeclaration(
                        PredefinedType(
                            Token(SyntaxKind.IntKeyword)
                        ),
                        Identifier("FieldCount")
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        ArrowExpressionClause(
                            LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                Literal(fieldCount)
                            )
                        )
                    )
                    .WithSemicolonToken(
                        Token(SyntaxKind.SemicolonToken)
                    )
                )
            ;

            classDeclarationSyntax =
                classDeclarationSyntax.WithLeadingTrivia(
                    CreateSummaryToken(doc, aliases.ToArray())
                )
            ;

            return classDeclarationSyntax;
        }

        internal static ClassDeclarationSyntax CreateErrorClass(string name, string errorMessage, int fieldCount, string avro, string doc, IEnumerable<string> aliases)
        {
            var classDeclarationSyntax =
                ClassDeclaration(name)
                .AddModifiers(Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SimpleBaseType(
                        ParseTypeName(typeof(IAvroError).FullName)
                    ),
                    SimpleBaseType(
                        ParseTypeName(typeof(IAvroRecord).FullName)
                    )
                )
                .AddMembers(
                    FieldDeclaration(
                        VariableDeclaration(
                            IdentifierName(nameof(ErrorSchema))
                        )
                        .WithVariables(
                            SingletonSeparatedList(
                                VariableDeclarator(
                                    Identifier("_SCHEMA")
                                )
                                .WithInitializer(
                                    EqualsValueClause(
                                        InvocationExpression(
                                            MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                IdentifierName(nameof(AvroParser)),
                                                GenericName(
                                                    Identifier(nameof(AvroParser.ReadSchema))
                                                )
                                                .WithTypeArgumentList(
                                                    TypeArgumentList(
                                                        SingletonSeparatedList<TypeSyntax>(
                                                            IdentifierName(nameof(ErrorSchema))
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                        .WithArgumentList(
                                            ArgumentList(
                                                SingletonSeparatedList(
                                                    Argument(
                                                        LiteralExpression(
                                                            SyntaxKind.StringLiteralExpression,
                                                            Literal(avro)
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
                        TokenList(
                            new[]{
                                Token(SyntaxKind.PublicKeyword),
                                Token(SyntaxKind.StaticKeyword),
                                Token(SyntaxKind.ReadOnlyKeyword)
                            }
                        )
                    ),
                    ConstructorDeclaration(
                        Identifier(name)
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithParameterList(
                        ParameterList(
                            SingletonSeparatedList(
                                Parameter(
                                    Identifier("errorMessage")
                                )
                                .WithType(
                                    PredefinedType(
                                        Token(SyntaxKind.StringKeyword)
                                    )
                                )
                            )
                        )
                    )
                    .WithBody(
                        Block(
                            SingletonList<StatementSyntax>(
                                ExpressionStatement(
                                    AssignmentExpression(
                                        SyntaxKind.SimpleAssignmentExpression,
                                        IdentifierName(nameof(IAvroError.Exception)),
                                        ObjectCreationExpression(
                                            IdentifierName(nameof(AvroException))
                                        )
                                        .WithArgumentList(
                                            ArgumentList(
                                                SingletonSeparatedList(
                                                    Argument(
                                                        IdentifierName("errorMessage")
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    ),
                    ConstructorDeclaration(
                        Identifier(name)
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithParameterList(
                        ParameterList(
                            SingletonSeparatedList(
                                Parameter(
                                    Identifier("exception")
                                )
                                .WithType(
                                    IdentifierName(nameof(AvroException))
                                )
                            )
                        )
                    )
                    .WithBody(
                        Block(
                            SingletonList<StatementSyntax>(
                                ExpressionStatement(
                                    AssignmentExpression(
                                        SyntaxKind.SimpleAssignmentExpression,
                                        IdentifierName(nameof(IAvroError.Exception)),
                                        IdentifierName("exception")
                                    )
                                )
                            )
                        )
                    ),
                    PropertyDeclaration(
                        IdentifierName(nameof(RecordSchema)),
                        Identifier("Schema")
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        ArrowExpressionClause(
                            IdentifierName("_SCHEMA")
                        )
                    )
                    .WithSemicolonToken(
                        Token(SyntaxKind.SemicolonToken)
                    ),
                    PropertyDeclaration(
                        IdentifierName(nameof(AvroException)),
                        Identifier(nameof(IAvroError.Exception))
                    )
                    .AddModifiers(
                        Token(SyntaxKind.PublicKeyword)
                    )
                    .WithAccessorList(
                        AccessorList()
                            .AddAccessors(
                                AccessorDeclaration(
                                    SyntaxKind.GetAccessorDeclaration
                                )
                                .WithSemicolonToken(
                                    Token(SyntaxKind.SemicolonToken)
                                ),
                                AccessorDeclaration(
                                    SyntaxKind.SetAccessorDeclaration
                                )
                                .WithModifiers(
                                    TokenList(
                                        Token(SyntaxKind.PrivateKeyword)
                                    )
                                )
                                .WithSemicolonToken(
                                    Token(SyntaxKind.SemicolonToken)
                                )
                            )
                    ),
                    PropertyDeclaration(
                        PredefinedType(
                            Token(SyntaxKind.IntKeyword)
                        ),
                        Identifier("FieldCount")
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithExpressionBody(
                        ArrowExpressionClause(
                            LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                Literal(fieldCount)
                            )
                        )
                    )
                    .WithSemicolonToken(
                        Token(SyntaxKind.SemicolonToken)
                    )
                )
            ;

            classDeclarationSyntax =
                classDeclarationSyntax.WithLeadingTrivia(
                    CreateSummaryToken(doc, aliases.ToArray())
                )
            ;

            return classDeclarationSyntax;
        }

        internal static ClassDeclarationSyntax CreateFixedClass(string name, string avro, int size, IEnumerable<string> aliases)
        {
            var classDeclarationSyntax =
                ClassDeclaration(name)
                .AddModifiers(Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SimpleBaseType(
                        ParseTypeName(typeof(IAvroFixed).FullName)
                    )
                )
                .WithMembers(
                    List(
                        new MemberDeclarationSyntax[]{
                            FieldDeclaration(
                                VariableDeclaration(
                                    IdentifierName(nameof(FixedSchema))
                                )
                                .WithVariables(
                                    SingletonSeparatedList(
                                        VariableDeclarator(
                                            Identifier("_SCHEMA")
                                        )
                                        .WithInitializer(
                                            EqualsValueClause(
                                                InvocationExpression(
                                                    MemberAccessExpression(
                                                        SyntaxKind.SimpleMemberAccessExpression,
                                                        IdentifierName(nameof(AvroParser)),
                                                        GenericName(
                                                            Identifier(nameof(AvroParser.ReadSchema))
                                                        )
                                                        .WithTypeArgumentList(
                                                            TypeArgumentList(
                                                                SingletonSeparatedList<TypeSyntax>(
                                                                    IdentifierName(nameof(FixedSchema))
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                                .WithArgumentList(
                                                    ArgumentList(
                                                        SingletonSeparatedList(
                                                            Argument(
                                                                LiteralExpression(
                                                                    SyntaxKind.StringLiteralExpression,
                                                                    Literal(avro)
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
                                TokenList(
                                    new[]{
                                        Token(SyntaxKind.PublicKeyword),
                                        Token(SyntaxKind.StaticKeyword),
                                        Token(SyntaxKind.ReadOnlyKeyword)
                                    }
                                )
                            ),
                            FieldDeclaration(
                                VariableDeclaration(
                                    PredefinedType(
                                        Token(SyntaxKind.IntKeyword)
                                    )
                                )
                                .WithVariables(
                                    SingletonSeparatedList(
                                        VariableDeclarator(
                                            Identifier("_SIZE")
                                        )
                                        .WithInitializer(
                                            EqualsValueClause(
                                                LiteralExpression(
                                                    SyntaxKind.NumericLiteralExpression,
                                                    Literal(size)
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            .WithModifiers(
                                TokenList(
                                    new []{
                                        Token(SyntaxKind.PublicKeyword),
                                        Token(SyntaxKind.ConstKeyword)
                                    }
                                )
                            ),
                            FieldDeclaration(
                                VariableDeclaration(
                                    ArrayType(
                                        PredefinedType(
                                            Token(SyntaxKind.ByteKeyword)
                                        )
                                    )
                                    .WithRankSpecifiers(
                                        SingletonList(
                                            ArrayRankSpecifier(
                                                SingletonSeparatedList<ExpressionSyntax>(
                                                    OmittedArraySizeExpression()
                                                )
                                            )
                                        )
                                    )
                                )
                                .WithVariables(
                                    SingletonSeparatedList(
                                        VariableDeclarator(
                                            Identifier("_value")
                                        )
                                    )
                                )
                            )
                            .WithModifiers(
                                TokenList(
                                    new []{
                                        Token(SyntaxKind.PrivateKeyword),
                                        Token(SyntaxKind.ReadOnlyKeyword)
                                    }
                                )
                            ),
                            ConstructorDeclaration(
                                Identifier(name)
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithBody(
                                Block(
                                    SingletonList<StatementSyntax>(
                                        ExpressionStatement(
                                            AssignmentExpression(
                                                SyntaxKind.SimpleAssignmentExpression,
                                                IdentifierName("_value"),
                                                ArrayCreationExpression(
                                                    ArrayType(
                                                        PredefinedType(
                                                            Token(SyntaxKind.ByteKeyword)
                                                        )
                                                    )
                                                    .WithRankSpecifiers(
                                                        SingletonList(
                                                            ArrayRankSpecifier(
                                                                SingletonSeparatedList<ExpressionSyntax>(
                                                                    IdentifierName("_SIZE")
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            ),
                            ConstructorDeclaration(
                                Identifier(name)
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithParameterList(
                                ParameterList(
                                    SingletonSeparatedList(
                                        Parameter(
                                            Identifier("value")
                                        )
                                        .WithType(
                                            ArrayType(
                                                PredefinedType(
                                                    Token(SyntaxKind.ByteKeyword)
                                                )
                                            )
                                            .WithRankSpecifiers(
                                                SingletonList(
                                                    ArrayRankSpecifier(
                                                        SingletonSeparatedList<ExpressionSyntax>(
                                                            OmittedArraySizeExpression()
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            .WithBody(
                                Block(
                                    IfStatement(
                                        BinaryExpression(
                                            SyntaxKind.NotEqualsExpression,
                                            MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                IdentifierName("value"),
                                                IdentifierName(nameof(Array.Length))
                                            ),
                                            IdentifierName("_SIZE")
                                        ),
                                        ThrowStatement(
                                            ObjectCreationExpression(
                                                IdentifierName(nameof(ArgumentException))
                                            )
                                            .WithArgumentList(
                                                ArgumentList(
                                                    SingletonSeparatedList(
                                                        Argument(
                                                            InterpolatedStringExpression(
                                                                Token(SyntaxKind.InterpolatedStringStartToken)
                                                            )
                                                            .WithContents(
                                                                List(
                                                                    new InterpolatedStringContentSyntax[]{
                                                                        InterpolatedStringText()
                                                                        .WithTextToken(
                                                                            Token(
                                                                                TriviaList(),
                                                                                SyntaxKind.InterpolatedStringTextToken,
                                                                                "Array must be of size: ",
                                                                                "Array must be of size: ",
                                                                                TriviaList()
                                                                            )
                                                                        ),
                                                                        Interpolation(
                                                                            IdentifierName("_SIZE")
                                                                        )
                                                                    }
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    ),
                                    ExpressionStatement(
                                        AssignmentExpression(
                                            SyntaxKind.SimpleAssignmentExpression,
                                            IdentifierName("_value"),
                                            IdentifierName("value")
                                        )
                                    )
                                )
                            ),
                            PropertyDeclaration(
                                IdentifierName(nameof(FixedSchema)),
                                Identifier("Schema")
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithExpressionBody(
                                ArrowExpressionClause(
                                    IdentifierName("_SCHEMA")
                                )
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            ),
                            PropertyDeclaration(
                                PredefinedType(
                                    Token(SyntaxKind.IntKeyword)
                                ),
                                Identifier(nameof(IAvroFixed.Size))
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithExpressionBody(
                                ArrowExpressionClause(
                                    IdentifierName("_SIZE")
                                )
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            ),
                            IndexerDeclaration(
                                PredefinedType(
                                    Token(SyntaxKind.ByteKeyword)
                                )
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithParameterList(
                                BracketedParameterList(
                                    SingletonSeparatedList(
                                        Parameter(
                                            Identifier("i")
                                        )
                                        .WithType(
                                            PredefinedType(
                                                Token(SyntaxKind.IntKeyword)
                                            )
                                        )
                                    )
                                )
                            )
                            .WithAccessorList(
                                AccessorList(
                                    List(
                                        new AccessorDeclarationSyntax[]{
                                            AccessorDeclaration(
                                                SyntaxKind.GetAccessorDeclaration
                                            )
                                            .WithExpressionBody(
                                                ArrowExpressionClause(
                                                    ElementAccessExpression(
                                                        IdentifierName("_value")
                                                    )
                                                    .WithArgumentList(
                                                        BracketedArgumentList(
                                                            SingletonSeparatedList(
                                                                Argument(
                                                                    IdentifierName("i")
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                            .WithSemicolonToken(
                                                Token(SyntaxKind.SemicolonToken)
                                            ),
                                            AccessorDeclaration(
                                                SyntaxKind.SetAccessorDeclaration
                                            )
                                            .WithExpressionBody(
                                                ArrowExpressionClause(
                                                    AssignmentExpression(
                                                        SyntaxKind.SimpleAssignmentExpression,
                                                        ElementAccessExpression(
                                                            IdentifierName("_value")
                                                        )
                                                        .WithArgumentList(
                                                            BracketedArgumentList(
                                                                SingletonSeparatedList(
                                                                    Argument(
                                                                        IdentifierName("i")
                                                                    )
                                                                )
                                                            )
                                                        ),
                                                        IdentifierName("value")
                                                    )
                                                )
                                            )
                                            .WithSemicolonToken(
                                                Token(SyntaxKind.SemicolonToken)
                                            )
                                        }
                                    )
                                )
                            ),
                            MethodDeclaration(
                                PredefinedType(
                                    Token(SyntaxKind.BoolKeyword)
                                ),
                                Identifier(nameof(object.Equals))
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithParameterList(
                                ParameterList(
                                    SingletonSeparatedList(
                                        Parameter(
                                            Identifier("other")
                                        )
                                        .WithType(
                                            IdentifierName(nameof(IAvroFixed))
                                        )
                                    )
                                )
                            )
                            .WithBody(
                                Block(
                                    IfStatement(
                                        BinaryExpression(
                                            SyntaxKind.NotEqualsExpression,
                                            IdentifierName(nameof(IAvroFixed.Size)),
                                            MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                IdentifierName("other"),
                                                IdentifierName(nameof(IAvroFixed.Size))
                                            )
                                        ),
                                        ReturnStatement(
                                            LiteralExpression(
                                                SyntaxKind.FalseLiteralExpression
                                            )
                                        )
                                    ),
                                    ForStatement(
                                        IfStatement(
                                            BinaryExpression(
                                                SyntaxKind.NotEqualsExpression,
                                                ElementAccessExpression(
                                                    ThisExpression()
                                                )
                                                .WithArgumentList(
                                                    BracketedArgumentList(
                                                        SingletonSeparatedList(
                                                            Argument(
                                                                IdentifierName("i")
                                                            )
                                                        )
                                                    )
                                                ),
                                                ElementAccessExpression(
                                                    IdentifierName("other")
                                                )
                                                .WithArgumentList(
                                                    BracketedArgumentList(
                                                        SingletonSeparatedList(
                                                            Argument(
                                                                IdentifierName("i")
                                                            )
                                                        )
                                                    )
                                                )
                                            ),
                                            ReturnStatement(
                                                LiteralExpression(
                                                    SyntaxKind.FalseLiteralExpression
                                                )
                                            )
                                        )
                                    )
                                    .WithDeclaration(
                                        VariableDeclaration(
                                            PredefinedType(
                                                Token(SyntaxKind.IntKeyword)
                                            )
                                        )
                                        .WithVariables(
                                            SingletonSeparatedList(
                                                VariableDeclarator(
                                                    Identifier("i")
                                                )
                                                .WithInitializer(
                                                    EqualsValueClause(
                                                        LiteralExpression(
                                                            SyntaxKind.NumericLiteralExpression,
                                                            Literal(0)
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                    .WithCondition(
                                        BinaryExpression(
                                            SyntaxKind.LessThanExpression,
                                            IdentifierName("i"),
                                            IdentifierName(nameof(IAvroFixed.Size))
                                        )
                                    )
                                    .WithIncrementors(
                                        SingletonSeparatedList<ExpressionSyntax>(
                                            PostfixUnaryExpression(
                                                SyntaxKind.PostIncrementExpression,
                                                IdentifierName("i")
                                            )
                                        )
                                    ),
                                    ReturnStatement(
                                        LiteralExpression(
                                            SyntaxKind.TrueLiteralExpression
                                        )
                                    )
                                )
                            ),
                            MethodDeclaration(
                                GenericName(
                                    Identifier(nameof(IEnumerator))
                                )
                                .WithTypeArgumentList(
                                    TypeArgumentList(
                                        SingletonSeparatedList<TypeSyntax>(
                                            PredefinedType(
                                                Token(SyntaxKind.ByteKeyword)
                                            )
                                        )
                                    )
                                ),
                                Identifier(nameof(IEnumerable.GetEnumerator))
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithBody(
                                Block(
                                    SingletonList<StatementSyntax>(
                                        ForEachStatement(
                                            IdentifierName("var"),
                                            Identifier("b"),
                                            IdentifierName("_value"),
                                            YieldStatement(
                                                SyntaxKind.YieldReturnStatement,
                                                IdentifierName("b")
                                            )
                                        )
                                    )
                                )
                            ),
                            MethodDeclaration(
                                IdentifierName(nameof(IEnumerator)),
                                Identifier(nameof(IEnumerable.GetEnumerator))
                            )
                            .WithExplicitInterfaceSpecifier(
                                ExplicitInterfaceSpecifier(
                                    IdentifierName(nameof(IEnumerable))
                                )
                            )
                            .WithExpressionBody(
                                ArrowExpressionClause(
                                    InvocationExpression(
                                        IdentifierName(nameof(IEnumerable.GetEnumerator))
                                    )
                                )
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            ),
                            ConversionOperatorDeclaration(
                                Token(SyntaxKind.ImplicitKeyword),
                                IdentifierName(name)
                            )
                            .WithModifiers(
                                TokenList(
                                    new []{
                                        Token(SyntaxKind.PublicKeyword),
                                        Token(SyntaxKind.StaticKeyword)
                                    }
                                )
                            )
                            .WithParameterList(
                                ParameterList(
                                    SingletonSeparatedList(
                                        Parameter(
                                            Identifier("value")
                                        )
                                        .WithType(
                                            ArrayType(
                                                PredefinedType(
                                                    Token(SyntaxKind.ByteKeyword)
                                                )
                                            )
                                            .WithRankSpecifiers(
                                                SingletonList(
                                                    ArrayRankSpecifier(
                                                        SingletonSeparatedList<ExpressionSyntax>(
                                                            OmittedArraySizeExpression()
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            .WithExpressionBody(
                                ArrowExpressionClause(
                                    ObjectCreationExpression(
                                        IdentifierName(name)
                                    )
                                    .WithArgumentList(
                                        ArgumentList(
                                            SingletonSeparatedList(
                                                Argument(
                                                    IdentifierName("value")
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            ),
                            ConversionOperatorDeclaration(
                                Token(SyntaxKind.ExplicitKeyword),
                                ArrayType(
                                    PredefinedType(
                                        Token(SyntaxKind.ByteKeyword)
                                    )
                                )
                                .WithRankSpecifiers(
                                    SingletonList(
                                        ArrayRankSpecifier(
                                            SingletonSeparatedList<ExpressionSyntax>(
                                                OmittedArraySizeExpression()
                                            )
                                        )
                                    )
                                )
                            )
                            .WithModifiers(
                                TokenList(
                                    new []{
                                        Token(SyntaxKind.PublicKeyword),
                                        Token(SyntaxKind.StaticKeyword)
                                    }
                                )
                            )
                            .WithParameterList(
                                ParameterList(
                                    SingletonSeparatedList(
                                        Parameter(
                                            Identifier("value")
                                        )
                                        .WithType(
                                            IdentifierName(name)
                                        )
                                    )
                                )
                            )
                            .WithExpressionBody(
                                ArrowExpressionClause(
                                    MemberAccessExpression(
                                        SyntaxKind.SimpleMemberAccessExpression,
                                        IdentifierName("value"),
                                        IdentifierName("_value")
                                    )
                                )
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            )
                        }
                    )
                )
            ;

            classDeclarationSyntax =
                classDeclarationSyntax.WithLeadingTrivia(
                    CreateSummaryToken(null, aliases.ToArray())
                )
            ;

            return classDeclarationSyntax;
        }

        internal static ClassDeclarationSyntax CreateProtocolClass(string name, string avro, string doc, IEnumerable<Message> messages)
        {
            var classDeclarationSyntax =
                ClassDeclaration(name)
                .AddModifiers(Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SimpleBaseType(
                        ParseTypeName(typeof(IProtocol).FullName)
                    )
                )
                .WithMembers(
                    List(
                        new MemberDeclarationSyntax[]{
                            FieldDeclaration(
                                VariableDeclaration(
                                    QualifiedName(
                                        IdentifierName(typeof(AvroSchema).Namespace),
                                        IdentifierName(nameof(AvroSchema))
                                    )
                                )
                                .WithVariables(
                                    SingletonSeparatedList(
                                        VariableDeclarator(
                                            Identifier("_SCHEMA")
                                        )
                                        .WithInitializer(
                                            EqualsValueClause(
                                                InvocationExpression(
                                                    MemberAccessExpression(
                                                        SyntaxKind.SimpleMemberAccessExpression,
                                                        MemberAccessExpression(
                                                            SyntaxKind.SimpleMemberAccessExpression,
                                                            IdentifierName(typeof(AvroProtocol).Namespace),
                                                            IdentifierName(typeof(AvroProtocol).Name)
                                                        ),
                                                        IdentifierName(nameof(AvroProtocol.Parse))
                                                    )
                                                )
                                                .WithArgumentList(
                                                    ArgumentList(
                                                        SingletonSeparatedList(
                                                            Argument(
                                                                LiteralExpression(
                                                                    SyntaxKind.StringLiteralExpression,
                                                                    Literal(avro)
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
                                TokenList(
                                    new []{
                                        Token(SyntaxKind.PublicKeyword),
                                        Token(SyntaxKind.StaticKeyword),
                                        Token(SyntaxKind.ReadOnlyKeyword)
                                    }
                                )
                            )
                        }
                    )
                );
            return classDeclarationSyntax;
        }

        internal static PropertyDeclarationSyntax CreateClassProperty(RecordSchema.Field field)
        {
            var systemType = GetSystemType(field.Type);
            var propertyDeclaration =
                PropertyDeclaration(
                    ParseTypeName(systemType),
                    field.Name
                )
                .AddModifiers(
                    Token(SyntaxKind.PublicKeyword)
                )
                .WithAccessorList(
                    AccessorList()
                        .AddAccessors(
                            AccessorDeclaration(
                                SyntaxKind.GetAccessorDeclaration
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            ),
                            AccessorDeclaration(
                                SyntaxKind.SetAccessorDeclaration
                            )
                            .WithSemicolonToken(
                                Token(SyntaxKind.SemicolonToken)
                            )
                        )
                )
            ;

            if (field.Default != null)
                propertyDeclaration =
                    propertyDeclaration
                        .WithInitializer(
                            EqualsValueClause(
                                ParseExpression(
                                    GetSystemTypeInitialization(field.Type, field.Default)
                                )
                            )
                        )
                        .WithSemicolonToken(
                            Token(SyntaxKind.SemicolonToken)
                        )
                    ;


            propertyDeclaration =
                propertyDeclaration.WithLeadingTrivia(
                    CreateSummaryToken(field.Doc, field.Aliases.ToArray())
                )
            ;

            return propertyDeclaration;
        }


        internal static ClassDeclarationSyntax AddMembersToClass(ClassDeclarationSyntax classDeclarationSyntax, params MemberDeclarationSyntax[] memberDeclarationSyntax)
        {
            return classDeclarationSyntax
                .AddMembers(
                    memberDeclarationSyntax
                )
            ;
        }

        internal static MemberDeclarationSyntax CreateRecordClassIndexer(IEnumerable<SwitchSectionSyntax> getSwitchSectionSyntaxes, IEnumerable<SwitchSectionSyntax> switchSectionSyntaxes, int maxRange)
        {
            return
                IndexerDeclaration(
                        PredefinedType(
                            Token(SyntaxKind.ObjectKeyword)
                        )
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithParameterList(
                        BracketedParameterList(
                            SingletonSeparatedList(
                                Parameter(
                                    Identifier("i")
                                )
                                .WithType(
                                    PredefinedType(
                                        Token(SyntaxKind.IntKeyword)
                                    )
                                )
                            )
                        )
                    )
                    .WithAccessorList(
                        AccessorList(
                            List(
                                new AccessorDeclarationSyntax[]{
                                    AccessorDeclaration(
                                        SyntaxKind.GetAccessorDeclaration
                                    )
                                    .WithBody(
                                        Block(
                                            SwitchStatement(
                                                IdentifierName("i")
                                            )
                                            .WithOpenParenToken(
                                                Token(SyntaxKind.OpenParenToken)
                                            )
                                            .WithCloseParenToken(
                                                Token(SyntaxKind.CloseParenToken)
                                            )
                                            .WithSections(
                                                List(
                                                    getSwitchSectionSyntaxes.Append(SwitchCaseDefaultIndexOutOfRange(maxRange))
                                                )
                                            )
                                        )
                                    ),
                                    AccessorDeclaration(
                                        SyntaxKind.SetAccessorDeclaration
                                    )
                                    .WithBody(
                                        Block(
                                            SwitchStatement(
                                                IdentifierName("i")
                                            )
                                            .WithOpenParenToken(
                                                Token(SyntaxKind.OpenParenToken)
                                            )
                                            .WithCloseParenToken(
                                                Token(SyntaxKind.CloseParenToken)
                                            )
                                            .WithSections(
                                                List(
                                                    switchSectionSyntaxes.Append(SwitchCaseDefaultIndexOutOfRange(maxRange))
                                                )
                                            )
                                        )
                                    )
                                }
                            )
                        )
                    )
                ;
        }

        internal static SwitchSectionSyntax SwitchCaseGetProperty(int caseLabel, string propertyName)
        {
            return
                SwitchSection()
                .WithLabels(
                    SingletonList<SwitchLabelSyntax>(
                        CaseSwitchLabel(
                            LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                Literal(caseLabel)
                            )
                        )
                    )
                )
                .WithStatements(
                    SingletonList<StatementSyntax>(
                        ReturnStatement(
                            IdentifierName(propertyName)
                        )
                    )
                )
            ;
        }

        internal static SwitchSectionSyntax SwitchCaseSetProperty(int caseLabel, string propertyName, string propertyType)
        {
            return
                SwitchSection()
                .WithLabels(
                    SingletonList<SwitchLabelSyntax>(
                        CaseSwitchLabel(
                            LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                Literal(caseLabel)
                            )
                        )
                    )
                )
                .WithStatements(
                    List(
                        new StatementSyntax[]{
                            ExpressionStatement(
                                AssignmentExpression(
                                    SyntaxKind.SimpleAssignmentExpression,
                                    IdentifierName(propertyName),
                                    CastExpression(
                                        ParseTypeName(propertyType),
                                        IdentifierName("value")
                                    )
                                )
                            ),
                            BreakStatement()
                        }
                    )
                )
            ;
        }

        private static SwitchSectionSyntax SwitchCaseDefaultIndexOutOfRange(int maxRange)
        {
            return
                SwitchSection()
                .WithLabels(
                    SingletonList<SwitchLabelSyntax>(
                        DefaultSwitchLabel()
                    )
                )
                .WithStatements(
                    SingletonList<StatementSyntax>(
                        ThrowStatement(
                            ObjectCreationExpression(
                                IdentifierName("IndexOutOfRangeException")
                            )
                            .WithArgumentList(
                                ArgumentList(
                                    SingletonSeparatedList(
                                        Argument(
                                            LiteralExpression(
                                                SyntaxKind.StringLiteralExpression,
                                                Literal($"Expected range: [0:{maxRange}].")
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

        internal static MemberDeclarationSyntax QualifyMember(MemberDeclarationSyntax memberDeclarationSyntax, string ns)
        {
            if (!string.IsNullOrEmpty(ns))
                return NamespaceDeclaration(ParseName(ns))
                    .AddMembers(memberDeclarationSyntax);
            else
                return memberDeclarationSyntax;
        }

        private static SyntaxTriviaList CreateSummaryToken(string doc, params string[] aliases)
        {
            var summaryList = new List<XmlNodeSyntax>();

            if (!string.IsNullOrEmpty(doc))
                summaryList.Add(
                    XmlText()
                    .WithTextTokens(
                        TokenList(
                            new[] {
                                XmlTextNewLine(
                                    TriviaList(),
                                    "\n",
                                    "\n",
                                    TriviaList()
                                ),
                                XmlTextLiteral(
                                    TriviaList(
                                        DocumentationCommentExterior("///")
                                    ),
                                    $" {doc}",
                                    $" {doc}",
                                    TriviaList()
                                ),
                                XmlTextNewLine(
                                    TriviaList(),
                                    "\n",
                                    "\n",
                                    TriviaList()
                                ),
                                XmlTextLiteral(
                                    TriviaList(
                                        DocumentationCommentExterior("///")
                                    ),
                                    " ",
                                    " ",
                                    TriviaList()
                                )
                            }
                        )
                    )
                );

            if (aliases.Count() > 0)
            {
                var aliasItems = new List<XmlNodeSyntax>();
                aliasItems.Add(
                    XmlExampleElement(
                        XmlText()
                        .WithTextTokens(
                            TokenList(
                                new[]{
                                XmlTextLiteral(
                                    TriviaList(),
                                    "  ",
                                    "  ",
                                    TriviaList()
                                ),
                                XmlTextNewLine(
                                    TriviaList(),
                                    "\n",
                                    "\n",
                                    TriviaList()
                                ),
                                XmlTextLiteral(
                                    TriviaList(
                                        DocumentationCommentExterior("    ///")
                                    ),
                                    " ",
                                    " ",
                                    TriviaList()
                                )
                                }
                            )
                        ),
                        XmlExampleElement(
                            SingletonList<XmlNodeSyntax>(
                                XmlText()
                                .WithTextTokens(
                                    TokenList(
                                        XmlTextLiteral(
                                            TriviaList(),
                                            "Aliases",
                                            "Aliases",
                                            TriviaList()
                                        )
                                    )
                                )
                            )
                        )
                        .WithStartTag(
                            XmlElementStartTag(
                                XmlName(
                                    Identifier("term")
                                )
                            )
                        )
                        .WithEndTag(
                            XmlElementEndTag(
                                XmlName(
                                    Identifier("term")
                                )
                            )
                        ),
                        XmlText()
                        .WithTextTokens(
                            TokenList(
                                new[]{
                                XmlTextLiteral(
                                    TriviaList(),
                                    "  ",
                                    "  ",
                                    TriviaList()
                                ),
                                XmlTextNewLine(
                                    TriviaList(),
                                    "\n",
                                    "\n",
                                    TriviaList()
                                ),
                                XmlTextLiteral(
                                    TriviaList(
                                        DocumentationCommentExterior("    ///")
                                    ),
                                    " ",
                                    " ",
                                    TriviaList()
                                )
                                }
                            )
                        )
                    )
                    .WithStartTag(
                        XmlElementStartTag(
                            XmlName(
                                Identifier("listheader")
                            )
                        )
                    )
                    .WithEndTag(
                        XmlElementEndTag(
                            XmlName(
                                Identifier("listheader")
                            )
                        )
                    )
                );

                foreach (var alias in aliases)
                    aliasItems.Add(
                        XmlExampleElement(
                            XmlText()
                            .WithTextTokens(
                                TokenList(
                                    new[]{
                                        XmlTextNewLine(
                                            TriviaList(),
                                            "\n",
                                            "\n",
                                            TriviaList()
                                        ),
                                        XmlTextLiteral(
                                            TriviaList(
                                                DocumentationCommentExterior("    ///")
                                            ),
                                            " ",
                                            " ",
                                            TriviaList()
                                        )
                                    }
                                )
                            ),
                            XmlExampleElement(
                                SingletonList<XmlNodeSyntax>(
                                    XmlText()
                                    .WithTextTokens(
                                        TokenList(
                                            XmlTextLiteral(
                                                TriviaList(),
                                                alias,
                                                alias,
                                                TriviaList()
                                            )
                                        )
                                    )
                                )
                            )
                            .WithStartTag(
                                XmlElementStartTag(
                                    XmlName(
                                        Identifier("term")
                                    )
                                )
                            )
                            .WithEndTag(
                                XmlElementEndTag(
                                    XmlName(
                                        Identifier("term")
                                    )
                                )
                            ),
                            XmlText()
                            .WithTextTokens(
                                TokenList(
                                    new[]{
                                        XmlTextNewLine(
                                            TriviaList(),
                                            "\n",
                                            "\n",
                                            TriviaList()
                                        ),
                                        XmlTextLiteral(
                                            TriviaList(
                                                DocumentationCommentExterior("    ///")
                                            ),
                                            " ",
                                            " ",
                                            TriviaList()
                                        )
                                    }
                                )
                            )
                        )
                        .WithStartTag(
                            XmlElementStartTag(
                                XmlName(
                                    Identifier("item")
                                )
                            )
                        )
                        .WithEndTag(
                            XmlElementEndTag(
                                XmlName(
                                    Identifier("item")
                                )
                            )
                        )
                    );

                summaryList.Add(
                    XmlExampleElement(
                        aliasItems.ToArray()
                    )
                    .WithStartTag(
                        XmlElementStartTag(
                            XmlName(
                                Identifier("list")
                            )
                        )
                        .WithAttributes(
                            SingletonList<XmlAttributeSyntax>(
                                XmlTextAttribute(
                                    XmlName(
                                        Identifier("type")
                                    ),
                                    Token(SyntaxKind.DoubleQuoteToken),
                                    Token(SyntaxKind.DoubleQuoteToken)
                                )
                                .WithTextTokens(
                                    TokenList(
                                        XmlTextLiteral(
                                            TriviaList(),
                                            "bullet",
                                            "bullet",
                                            TriviaList()
                                        )
                                    )
                                )
                            )
                        )
                    )
                    .WithEndTag(
                        XmlElementEndTag(
                            XmlName(
                                Identifier("list")
                            )
                        )
                    )
                );
            }

            return
                TriviaList(
                    Trivia(
                        DocumentationCommentTrivia(
                            SyntaxKind.SingleLineDocumentationCommentTrivia,
                            List(
                                new XmlNodeSyntax[]{
                                    XmlText()
                                    .WithTextTokens(
                                        TokenList(
                                            XmlTextLiteral(
                                                TriviaList(
                                                    DocumentationCommentExterior("///")
                                                ),
                                                " ",
                                                " ",
                                                TriviaList()
                                            )
                                        )
                                    ),
                                    XmlExampleElement(
                                        List(
                                            summaryList.ToArray()
                                        )
                                    )
                                    .WithStartTag(
                                        XmlElementStartTag(
                                            XmlName(
                                                Identifier("summary")
                                            )
                                        )
                                    )
                                    .WithEndTag(
                                        XmlElementEndTag(
                                            XmlName(
                                                Identifier("summary")
                                            )
                                        )
                                    ),
                                    XmlText()
                                    .WithTextTokens(
                                        TokenList(
                                            XmlTextNewLine(
                                                TriviaList(),
                                                "\n",
                                                "\n",
                                                TriviaList()
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
                CompilationUnit()
                .WithUsings(
                    List(
                        new UsingDirectiveSyntax[]{
                            UsingDirective(
                                IdentifierName("Avro")
                            ),
                            UsingDirective(
                                IdentifierName("Avro.")
                            ),
                            UsingDirective(
                                IdentifierName("System")
                            ),
                            UsingDirective(
                                IdentifierName("System.Collections.Generic")
                            )
                        }
                    )
                )
                .WithMembers(
                    SingletonList(
                        memberDeclarationSyntax
                    )
                );
        }

        internal static CompilationUnitSyntax CreateCompileUnit(IEnumerable<MemberDeclarationSyntax> memberDeclarationSyntaxes)
        {
            return
                CompilationUnit()
                .WithUsings(
                    List(
                        new UsingDirectiveSyntax[]{
                            UsingDirective(
                                IdentifierName("Avro")
                            ),
                            UsingDirective(
                                IdentifierName("Avro.Schema")
                            ),
                            UsingDirective(
                                IdentifierName("Avro.Types")
                            ),
                            UsingDirective(
                                IdentifierName("System")
                            ),
                            UsingDirective(
                                IdentifierName("System.Collections")
                            ),
                            UsingDirective(
                                IdentifierName("System.Collections.Generic")
                            )
                        }
                    )
                )
                .WithMembers(
                    List(
                        memberDeclarationSyntaxes
                    )
                );
        }

        public static Assembly Compile(string assemblyName, string code, out XmlDocument xmlDocumentation)
        {
            var references = new[] {
                MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof(IAvroRecord).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            };

            var compilationUnit = ParseCompilationUnit(code);

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

                var compilationErrors =
                    result.Diagnostics.Where(
                        diagnostic =>
                        diagnostic.Severity == DiagnosticSeverity.Error
                    )
                ;

                if (compilationErrors.Any())
                {
                    var firstError = compilationErrors.First();
                    var errorNumber = firstError.Id;
                    var errorDescription = firstError.GetMessage();
                    var firstErrorMessage = $"{errorNumber}: {errorDescription};";
                    throw new CodeGenException($"Compilation failed, first error is: {firstErrorMessage}");
                }

                ds.Seek(0, SeekOrigin.Begin);
                xmlDocumentation = new XmlDocument();
                xmlDocumentation.Load(ds);

                var assembly = Assembly.Load(ms.ToArray());
                return assembly;
            }
        }

        public static string GetSystemType(AvroSchema schema, bool nullable = false)
        {
            switch (schema)
            {
                case NullSchema _:
                    return "object";

                case BooleanSchema _:
                    if (nullable)
                        return "bool?";
                    else
                        return "bool";

                case IntSchema _:
                    if (nullable)
                        return "int?";
                    else
                        return "int";

                case LongSchema _:
                    if (nullable)
                        return "long?";
                    else
                        return "long";

                case FloatSchema _:
                    if (nullable)
                        return "float?";
                    else
                        return "float";

                case DoubleSchema _:
                    if (nullable)
                        return "double?";
                    else
                        return "double";

                case BytesSchema _:
                    return "byte[]";

                case StringSchema _:
                    return "string";

                case ArraySchema _:
                    return GetSystemType(schema as ArraySchema);

                case MapSchema _:
                    return GetSystemType(schema as MapSchema);

                case FixedSchema _:
                case EnumSchema _:
                case ErrorSchema _:
                case RecordSchema _:
                    return GetSystemType(schema as NamedSchema);

                case UnionSchema _:
                    return GetSystemType(schema as UnionSchema);

                case DecimalSchema _:
                    if (nullable)
                        return "decimal?";
                    else
                        return "decimal";

                case TimeMillisSchema _:
                case TimeMicrosSchema _:
                case TimeNanosSchema _:
                    if (nullable)
                        return "TimeSpan?";
                    else
                        return "TimeSpan";

                case TimestampMillisSchema _:
                case TimestampMicrosSchema _:
                case TimestampNanosSchema _:
                case DateSchema _:
                    if (nullable)
                        return "DateTime?";
                    else
                        return "DateTime";

                case DurationSchema _:
                    if (nullable)
                        return "AvroDuration?";
                    else
                        return "AvroDuration";

                case UuidSchema _:
                    if (nullable)
                        return "Guid?";
                    else
                        return "Guid";

                case LogicalSchema _:
                    return GetSystemType(schema as LogicalSchema);

                default:
                    return "object";
            }
        }

        public static string GetSystemTypeInitialization(AvroSchema schema, JToken value)
        {
            var defaultInit = string.Empty;
            switch (schema)
            {
                case NullSchema _:
                    defaultInit = "null";
                    break;
                case BooleanSchema _:
                    defaultInit = value.ToString().ToLower();
                    break;
                case IntSchema _:
                    defaultInit = value.ToString();
                    break;
                case StringSchema _:
                    defaultInit = value.ToString();
                    break;
                case LongSchema _:
                    defaultInit = $"{value.ToString()}L";
                    break;
                case FloatSchema _:
                    defaultInit = $"{value.ToString()}F";
                    break;
                case DoubleSchema _:
                    defaultInit = $"{value.ToString()}D";
                    break;
                case BytesSchema _:
                    defaultInit = $"new byte[] {{{string.Join(", ", value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries).Select(r => $"0x{byte.Parse(r, System.Globalization.NumberStyles.HexNumber).ToString("X2")}"))}}}";
                    break;
                case ArraySchema a:
                    defaultInit = $"new List<{GetSystemType(a.Items)}>() {{{string.Join(", ", (value as JArray).Select(r => GetSystemTypeInitialization(a.Items, r)))}}}";
                    break;
                case MapSchema m:
                    defaultInit = $"new Dictionary<string, {GetSystemType(m.Values)}>() {{{string.Join(", ", (value as JObject).Properties().Select(r => $"{{\"{r.Name}\", {GetSystemTypeInitialization(m.Values, r.Value)}}}"))}}}";
                    break;
                case FixedSchema f:
                    defaultInit = $"new {GetSystemType(f)}({GetSystemTypeInitialization(new BytesSchema(), value)})";
                    break;
                case EnumSchema e:
                    defaultInit = $"{GetSystemType(e)}.{value.ToString().Trim('"')}";
                    break;
                case RecordSchema r:
                    var defaultFields =
                        from f in r
                        join p in (value as JObject).Properties() on f.Name equals p.Name
                        select new
                        {
                            Field = f,
                            Default = p.Value
                        }
                    ;

                    var defaultAssignment =
                        from d in defaultFields
                        select new
                        {
                            d.Field.Name,
                            Value =
                                GetSystemType(d.Field.Type) == "string" ?
                                $"\"{GetSystemTypeInitialization(d.Field.Type, d.Default)}\"" :
                                GetSystemTypeInitialization(d.Field.Type, d.Default)
                        }
                    ;
                    defaultInit = $"new {GetSystemType(r)}(){{{string.Join(", ", defaultAssignment.Select(f => $"{f.Name} = {f.Value}"))}}}";
                    break;
                case UnionSchema u:
                    defaultInit = GetSystemTypeInitialization(u[0], value);
                    break;
                case UuidSchema _:
                    defaultInit = $"new Guid({value.ToString()})";
                    break;
                case LogicalSchema l:
                    defaultInit = GetSystemTypeInitialization(l.Type, value);
                    break;
            }
            return defaultInit;
        }

        private static string GetSystemType(ArraySchema schema)
        {
            return $"IList<{GetSystemType(schema.Items)}>";
        }

        private static string GetSystemType(MapSchema schema)
        {
            return $"IDictionary<string, {GetSystemType((schema as MapSchema).Values)}>";
        }

        private static string GetSystemType(NamedSchema schema)
        {
            return schema.FullName;
        }

        private static string GetSystemType(LogicalSchema schema)
        {
            return GetSystemType(schema.Type);
        }

        private static string GetSystemType(UnionSchema schema)
        {
            switch (schema.Count())
            {
                case 1:
                    return GetSystemType(schema.First());
                case 2:
                    if (schema[0].GetType().Equals(typeof(NullSchema)))
                        return GetSystemType(schema[1], true);
                    else if (schema[1].GetType().Equals(typeof(NullSchema)))
                        return GetSystemType(schema[0], true);
                    else
                        return "object";
                default:
                    return "object";
            }
        }
    }
}
