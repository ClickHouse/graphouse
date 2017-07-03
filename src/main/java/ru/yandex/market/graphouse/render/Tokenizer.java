//package ru.yandex.market.graphouse.render;
//
//import org.jparsec.Parser;
//import org.jparsec.Parsers;
//import org.jparsec.Scanners;
//import org.jparsec.Terminals;
//
//import java.util.List;
//
///**
// * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
// * @date 25/02/2017
// */
//public class Tokenizer {
//    private static final Terminals OPERATORS = Terminals.operators("(", ")", ",");
//    private static final Parser<Void> IGNORED = Parsers.or(Scanners.WHITESPACES).skipMany();
//
//    private static final Parser<?> TOKENIZER = OPERATORS.tokenizer().cast().or(IGNORED);
//
//    private static final Parser<Double> NUMBER = Terminals.DecimalLiteral.PARSER.map(Double::valueOf);
//
//
//    public static void parse(String String) {
//
//    }
//
//    public static class Func {
//        private String name;
//        private List<?> subs;
//
//    }
//
//}
