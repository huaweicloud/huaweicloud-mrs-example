package com.huawei.fi.plugins.log;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Plugin(name = "logmask", category = "Converter")
@ConverterKeys({"m"})
public class ExampleMaskingConverter extends LogEventPatternConverter {

    private static final String NAME = "m";

    private Pattern sensitivePattern;

    /*
    <fields>
        <field name="name" max="30" prefixNoMaskLen ="1" mask="#"/>
        <field name="idNo" max="18" prefixNoMaskLen ="1"/>
        <field name="bankCardNo" max="20" suffixNoMaskLen ="4"/>
        <field name="mobile" max="18" prefixNoMaskLen="3" suffixNoMaskLen="4"/>
    </fields>
    */
    private Map<String, Sensitive> sensitives = new HashMap<String, Sensitive>();

    public ExampleMaskingConverter(String[] options) {
        super(NAME, NAME);

        {
            Sensitive s1 = new Sensitive("name", (short) 30, (short) 1, (short) 0);
            sensitives.put("name", s1);
            Sensitive s2 = new Sensitive("idNo", (short) 18, (short) 1, (short) 0);
            sensitives.put("idNo", s2);
            Sensitive s3 = new Sensitive("bankCardNo", (short) 20, (short) 0, (short) 4);
            sensitives.put("bankCardNo", s3);
            Sensitive s4 = new Sensitive("mobile", (short) 11, (short) 3, (short) 4);
            sensitives.put("mobile", s4);
        }

        StringBuffer sb = new StringBuffer();
        for (String key : sensitives.keySet()) {
            sb.append(key).append("|");
        }
        if (sb.lastIndexOf("|") > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if (sb.length() > 0) {
            sensitivePattern = Pattern.compile("\"(" + sb.toString() + ")\": \"([^\"]+)\"");
        }
    }

    public static ExampleMaskingConverter newInstance(final String[] options) {
        return new ExampleMaskingConverter(options);
    }

    @Override
    public void format(LogEvent event, StringBuilder outputMessage) {
        String message = event.getMessage().getFormattedMessage();
        outputMessage.append(mask(message));
    }

    private String mask(String message) {
        try {
            if (sensitivePattern != null) {
                StringBuffer buffer = new StringBuffer();
                Matcher matcher = sensitivePattern.matcher(message);
                while (matcher.find()) {
                    Sensitive sensitive = sensitives.get(matcher.group(1));
                    String strFieldVal = matcher.group(2);
                    int length = strFieldVal.length();
                    int totalNoMaskLen = sensitive.getPrefixNoMaskLen() + sensitive.getSuffixNoMaskLen();
                    if (totalNoMaskLen == 0) {
                        strFieldVal = fillMask(sensitive.getMask(), length);
                    }

                    if (totalNoMaskLen < length) {
                        StringBuilder sb = new StringBuilder();
                        for (int j = 0; j < strFieldVal.length(); j++) {
                            if (j < sensitive.getPrefixNoMaskLen()) {
                                sb.append(strFieldVal.charAt(j));
                                continue;
                            }
                            if (j > (strFieldVal.length() - sensitive.getSuffixNoMaskLen() - 1)) {
                                sb.append(strFieldVal.charAt(j));
                                continue;
                            }
                            sb.append(sensitive.getMask());
                        }
                        strFieldVal = sb.toString();
                    }
                    matcher.appendReplacement(buffer, "\"$1\": \"" + strFieldVal + "\"");
                }
                matcher.appendTail(buffer);
                return buffer.toString();
            }
        } catch (Exception e) {
            // Although if this fails, it may be better to not log the exception message
            // do nothing try to look for the log4j internal log method
        }
        return message;
    }

    private String fillMask(String maskStr, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(maskStr);
        }
        return sb.toString();
    }
}
