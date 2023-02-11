package com.huawei.fi.plugins.log;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Plugin(name = "logmask", category = "Converter")
@ConverterKeys({"m"})
public class ExampleMaskingConverterForXML extends LogEventPatternConverter {

    private static final String NAME = "m";

    private Pattern sensitivePattern;

    private static final String LOG_DESENSITIVE= "log4j2-desensitive.xml";

    private Map<String, Sensitive> sensitives = new HashMap<String, Sensitive>();

    public ExampleMaskingConverterForXML(String[] options) {
        super(NAME, NAME);

        URL url= this.getClass().getResource("/" + LOG_DESENSITIVE);
        if (url != null) {
            DesensitiveXmlReader parser= new DesensitiveXmlReader();
            List<Sensitive> list= parser.parse(url.getPath());
            StringBuffer sb= new StringBuffer();
            for (Sensitive s : list) {
                sb.append(s.getName()).append("|");
                sensitives.put(s.getName(), s);
            }
            if (sb.lastIndexOf("|") > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            if (sb.length() > 0) {
                sensitivePattern = Pattern.compile("\"(" + sb.toString() + ")\": \"([^\"]+)\"");
            }
        }
    }

    public static ExampleMaskingConverterForXML newInstance(final String[] options) {
        return new ExampleMaskingConverterForXML(options);
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
