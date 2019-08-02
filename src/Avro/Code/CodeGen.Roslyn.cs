using Avro.Protocols;
using Avro.Schemas;
using Avro.Specific;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
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
                        ParseTypeName(typeof(ISpecificRecord).FullName)
                    )
                )
                .AddMembers(
                    FieldDeclaration(
                        VariableDeclaration(
                            ParseTypeName(typeof(Schema).FullName)
                        )
                        .AddVariables(
                            VariableDeclarator(
                                Identifier("_SCHEMA")
                            ).WithInitializer(
                                EqualsValueClause(
                                    InvocationExpression(
                                        MemberAccessExpression(
                                            SyntaxKind.SimpleMemberAccessExpression,
                                            MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                IdentifierName(typeof(Schema).Namespace),
                                                IdentifierName(typeof(Schema).Name)
                                            ),
                                            IdentifierName(nameof(Schema.Parse))
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
                    .AddModifiers(
                        Token(SyntaxKind.PublicKeyword),
                        Token(SyntaxKind.StaticKeyword),
                        Token(SyntaxKind.ReadOnlyKeyword)
                    ),
                    PropertyDeclaration(
                        QualifiedName(
                            IdentifierName(typeof(Schema).Namespace),
                            IdentifierName(nameof(Schema))
                        ),
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
                        ParseTypeName(typeof(AvroRemoteException).FullName)
                    ),
                    SimpleBaseType(
                        ParseTypeName(typeof(ISpecificRecord).FullName)
                    )
                )
                .AddMembers(
                    FieldDeclaration(
                        VariableDeclaration(
                            ParseTypeName(typeof(Schema).FullName)
                        )
                        .AddVariables(
                            VariableDeclarator(
                                Identifier("_SCHEMA")
                            ).WithInitializer(
                                EqualsValueClause(
                                    InvocationExpression(
                                        MemberAccessExpression(
                                            SyntaxKind.SimpleMemberAccessExpression,
                                            MemberAccessExpression(
                                                SyntaxKind.SimpleMemberAccessExpression,
                                                IdentifierName(typeof(Schema).Namespace),
                                                IdentifierName(typeof(Schema).Name)
                                            ),
                                            IdentifierName(nameof(Schema.Parse))
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
                    .AddModifiers(
                        Token(SyntaxKind.PublicKeyword),
                        Token(SyntaxKind.StaticKeyword),
                        Token(SyntaxKind.ReadOnlyKeyword)
                    ),
                    ConstructorDeclaration(
                        Identifier(name)
                    )
                    .WithModifiers(
                        TokenList(
                            Token(SyntaxKind.PublicKeyword)
                        )
                    )
                    .WithInitializer(
                        ConstructorInitializer(
                            SyntaxKind.BaseConstructorInitializer,
                            ArgumentList(
                                SingletonSeparatedList(
                                    Argument(
                                        LiteralExpression(
                                            SyntaxKind.StringLiteralExpression,
                                            Literal(errorMessage)
                                        )
                                    )
                                )
                            )
                        )
                    )
                    .WithBody(
                        Block()
                    ),
                    PropertyDeclaration(
                        QualifiedName(
                            IdentifierName(typeof(Schema).Namespace),
                            IdentifierName(nameof(Schema))
                        ),
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

        internal static ClassDeclarationSyntax CreateFixedClass(string name, string avro, int size, IEnumerable<string> aliases)
        {
            var classDeclarationSyntax =
                ClassDeclaration(name)
                .AddModifiers(Token(SyntaxKind.PublicKeyword))
                .AddBaseListTypes(
                    SimpleBaseType(
                        ParseTypeName(typeof(ISpecificFixed).FullName)
                    )
                )
                .WithMembers(
                    List(
                        new MemberDeclarationSyntax[]{
                            FieldDeclaration(
                                VariableDeclaration(
                                    QualifiedName(
                                        IdentifierName(typeof(Schema).Namespace),
                                        IdentifierName(nameof(Schema))
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
                                                            IdentifierName(typeof(Schema).Namespace),
                                                            IdentifierName(typeof(Schema).Name)
                                                        ),
                                                        IdentifierName(nameof(Schema.Parse))
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
                                    ExpressionStatement(
                                        AssignmentExpression(
                                            SyntaxKind.SimpleAssignmentExpression,
                                            IdentifierName("Value"),
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
                            ),
                            PropertyDeclaration(
                                QualifiedName(
                                    IdentifierName(typeof(Schema).Namespace),
                                    IdentifierName(nameof(Schema))
                                ),
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
                                Identifier("FixedSize")
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
                            PropertyDeclaration(
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
                                ),
                                Identifier("Value")
                            )
                            .WithModifiers(
                                TokenList(
                                    Token(SyntaxKind.PublicKeyword)
                                )
                            )
                            .WithAccessorList(
                                AccessorList(
                                    List(
                                        new AccessorDeclarationSyntax[]{
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
                                        }
                                    )
                                )
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
                        ParseTypeName(typeof(ISpecificProtocol).FullName)
                    )
                )
                .WithMembers(
                    List(
                        new MemberDeclarationSyntax[]{
                            FieldDeclaration(
                                VariableDeclaration(
                                    QualifiedName(
                                        IdentifierName(typeof(Schema).Namespace),
                                        IdentifierName(nameof(Schema))
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
                                                            IdentifierName(typeof(Protocol).Namespace),
                                                            IdentifierName(typeof(Protocol).Name)
                                                        ),
                                                        IdentifierName(nameof(Protocol.Parse))
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

        internal static PropertyDeclarationSyntax CreateClassProperty(string name, string type, string doc, IEnumerable<string> aliases)
        {
            var propertyDeclaration =
                PropertyDeclaration(
                    ParseTypeName(type),
                    name
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

            propertyDeclaration =
                propertyDeclaration.WithLeadingTrivia(
                    CreateSummaryToken(doc, aliases.ToArray())
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

        internal static MemberDeclarationSyntax CreateClassGetMethod(IEnumerable<SwitchSectionSyntax> getSwitchSectionSyntaxes, int maxRange)
        {
            return
                MethodDeclaration(
                    PredefinedType(
                        Token(SyntaxKind.ObjectKeyword)
                    ),
                    Identifier("Get")
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
                                Identifier("fieldPos")
                            )
                            .WithType(
                                PredefinedType(
                                    Token(SyntaxKind.IntKeyword)
                                )
                            )
                        )
                    )
                )
                .WithBody(
                    Block(
                        SwitchStatement(
                            IdentifierName("fieldPos")
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
                )
            ;
        }

        internal static MemberDeclarationSyntax CreateClassSetMethod(IEnumerable<SwitchSectionSyntax> switchSectionSyntaxes, int maxRange)
        {
            return
                MethodDeclaration(
                    PredefinedType(
                        Token(SyntaxKind.VoidKeyword)
                    ),
                    Identifier("Put")
                )
                .WithModifiers(
                    TokenList(
                        Token(SyntaxKind.PublicKeyword)
                    )
                )
                .WithParameterList(
                    ParameterList(
                        SeparatedList<ParameterSyntax>(
                            new SyntaxNodeOrToken[]{
                                Parameter(
                                    Identifier("fieldPos")
                                )
                                .WithType(
                                    PredefinedType(
                                        Token(SyntaxKind.IntKeyword)
                                    )
                                ),
                                Token(SyntaxKind.CommaToken),
                                Parameter(
                                    Identifier("fieldValue")
                                )
                                .WithType(
                                    PredefinedType(
                                        Token(SyntaxKind.ObjectKeyword)
                                    )
                                )
                            }
                        )
                    )
                )
                .WithBody(
                    Block(
                        SwitchStatement(
                            IdentifierName("fieldPos")
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
                                        IdentifierName("fieldValue")
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
                                IdentifierName("Avro.Specific")
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
                                IdentifierName("Avro.Specific")
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
                    List(
                        memberDeclarationSyntaxes
                    )
                );
        }

        public static Assembly Compile(string assemblyName, string code, out XmlDocument xmlDocumentation)
        {
            var references = new[] {
                MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof(ISpecificRecord).GetTypeInfo().Assembly.Location),
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

        public static string GetSystemType(Schema schema, bool nullable = false)
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
                        return "ValueTuple<int, int, int>?";
                    else
                        return "ValueTuple<int, int, int>";

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
