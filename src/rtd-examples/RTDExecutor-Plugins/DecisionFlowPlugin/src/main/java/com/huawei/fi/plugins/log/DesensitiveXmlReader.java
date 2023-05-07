package com.huawei.fi.plugins.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DesensitiveXmlReader {
    private static final Logger LOG = LoggerFactory.getLogger(DesensitiveXmlReader.class);

    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();

    public List<Sensitive> parse(String filePath) {
        Document document = null;
        try {
            //DOM parser instance
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            //parse an XML file into a DOM tree
            document = builder.parse(new File(filePath));
            return doParse(document);
        } catch (Exception e) {
            LOG.error("Severe error occurred while parsing desensitive configuration.", e);
        }
        return new ArrayList<Sensitive>(0);
    }

    private List<Sensitive> doParse(Document document)  {
        Element rootElement = document.getDocumentElement();
        List<Sensitive> list= new ArrayList<Sensitive>();
        NodeList nodeList = rootElement.getElementsByTagName("field");
        if (nodeList != null) {
            for (int i = 0 ; i < nodeList.getLength(); i++) {
                Element element = (Element)nodeList.item(i);

                String name = element.getAttribute("name");
                Sensitive sensitive= new Sensitive(name);
                String mask = element.getAttribute("mask");
                if (mask != null && mask.length() > 0) {
                    sensitive.setMask(mask);
                }
                try {
                    String max = element.getAttribute("max");
                    if (max != null && max.length() > 0) {
                        sensitive.setSize(Short.parseShort(max));
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Desensitive configuration number format error for the element '" + name + "'.", e);
                }

                try {
                    String prefixNoMaskLen = element.getAttribute("prefixNoMaskLen");
                    if (prefixNoMaskLen != null && prefixNoMaskLen.length() > 0) {
                        sensitive.setPrefixNoMaskLen(Short.parseShort(prefixNoMaskLen));
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Desensitive configuration number format error for the element '" + name + "'.", e);
                }

                try {
                    String suffixNoMaskLen = element.getAttribute("suffixNoMaskLen");
                    if (suffixNoMaskLen != null && suffixNoMaskLen.length() > 0) {
                        sensitive.setSuffixNoMaskLen(Short.parseShort(suffixNoMaskLen));
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Desensitive configuration number format error for the element '" + name + "'.", e);
                }
                list.add(sensitive);
            }
        }
        return list;
    }
}