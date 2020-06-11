/*
 * @Author: wangran
 * @Date: 2020-06-09 16:11:58
 * @LastEditors: wangran
 * @LastEditTime: 2020-06-11 08:37:34
 */
package com.auto_generat.kettle.generat;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.kafka.producer.KafkaProducerMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.trans.steps.fileinput.text.TextFileFilter;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInputMeta;
import org.pentaho.di.trans.steps.scriptvalues_mod.ScriptValuesMetaMod;
import org.pentaho.di.trans.steps.scriptvalues_mod.ScriptValuesScript;

public class FileToKafkaTrans {
    public static FileToKafkaTrans fileToKafkaTrans;

    public static void main(String[] args) {
        try {
            StepPluginType.getInstance().getPluginFolders()
                    .add(new PluginFolder(
                            "E:\\迅雷下载\\pdi-ce-8.2.0.0-342\\data-integration\\plugins\\steps\\pentaho-kafka-producer",
                            false, true));
            KettleEnvironment.init();
            fileToKafkaTrans = new FileToKafkaTrans();
            TransMeta transMeta = fileToKafkaTrans.generateMyOwnTrans();
            String transXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" + transMeta.getXML(); // 拼接ktr文件的文件头
            String transName = "C:\\Users\\Administrator\\Desktop\\deskTopFolder\\generate_file_kafka.ktr";
            File file = new File(transName);
            FileUtils.writeStringToFile(file, transXml, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    /**
     * 生成一个转化,从txt文件中获取数据, 经过javascript处理, 再将整条数据存入kafka中
     * 
     * @return
     * @throws KettleXMLException
     */
    public TransMeta generateMyOwnTrans() throws KettleXMLException {
        System.out.println("************start to generate my own transformation***********");
        TransMeta transMeta = new TransMeta();
        transMeta.setName("file_kafka");
        // registry是给每个步骤生成一个标识Id用
        PluginRegistry registry = PluginRegistry.getInstance();
        // 文本输入
        TextFileInputMeta textInput = new TextFileInputMeta();// 定义Meta
        String textInputPluginId = registry.getPluginId(StepPluginType.class, textInput);// 生成id
        textInput.setDefault();// 配置默认属性
        String transPath = "E:\\迅雷下载\\pdi-ce-8.2.0.0-342\\data-integration\\export_file\\hello.txt";
        textInput.setFileName(new String[] { transPath });
        textInput.setFilter(new TextFileFilter[0]); // 过滤器
        textInput.content.fileFormat = "unix"; // 文件格式
        textInput.content.fileType = "CSV"; // 文件类型
        BaseFileField field = new BaseFileField();
        field.setName("pwd"); // 单条数据的键
        field.setTrimType(ValueMetaInterface.TRIM_TYPE_BOTH); // 去除空格方式
        field.setType("String");// 字段类型
        textInput.inputFields = new BaseFileField[] { field }; // 设置列赋值
        StepMeta TextInputMetaStep = new StepMeta(textInputPluginId, "text input", textInput);// 生成StepMeta
        TextInputMetaStep.setDraw(true);// 显示该控件
        TextInputMetaStep.setLocation(400, 300);// 显示位置, xy坐标
        transMeta.addStep(TextInputMetaStep);
        // js脚本配置
        ScriptValuesScript jsScript = new ScriptValuesScript(0, "Script0", "var pwd = pwd+\"hello\";");// 创建js脚本代码
        ScriptValuesMetaMod scriptValuesModMeta = new ScriptValuesMetaMod(); // 定义meta
        String scriptValuesModPluginId = registry.getPluginId(StepPluginType.class, scriptValuesModMeta);
        scriptValuesModMeta.setDefault();
        scriptValuesModMeta.setJSScripts(new ScriptValuesScript[] { jsScript });// 设置脚本
        scriptValuesModMeta.setFieldname(new String[] { "pwd" });// 输出列
        scriptValuesModMeta.setRename(new String[] { "pwd" });// 输出列别名
        /**
         * 字段类型: 0为None, 1为Number, 2为String, 3为Date, 4为Boolean, 5为Integer, 6为BinNumber,
         * 7为Serializable, 8为Binary, 9为Timestamp, 10为Internet Address
         */
        scriptValuesModMeta.setType(new int[] { 2 });// 根据上述配置字段类型
        scriptValuesModMeta.setLength(new int[] { -1 });// 长度, -1表示不进行设置
        scriptValuesModMeta.setPrecision(new int[] { -1 });// 精度, -1表示不进行设置
        scriptValuesModMeta.setReplace(new boolean[] { false });// 是否替换 "Fieldname"或"Rename to"值
        StepMeta javaScriptMetaStep = new StepMeta(scriptValuesModPluginId, "script option", scriptValuesModMeta);
        javaScriptMetaStep.setDraw(true);
        javaScriptMetaStep.setLocation(600, 300);
        transMeta.addStep(javaScriptMetaStep);
        // kafka 插件
        KafkaProducerMeta kafkaProducer = new KafkaProducerMeta();
        String KafkaProducerPluginId = registry.getPluginId(StepPluginType.class, kafkaProducer);
        kafkaProducer.setDefault();// 默认设置
        kafkaProducer.setMessageField("pwd");// 获取数据来源的字段
        kafkaProducer.setTopic("logstash_json");// topic
        kafkaProducer.getKafkaProperties().put("metadata.broker.list", "192.168.5.39:9092");// 对值进行引用传递设置kafka的ip:port
        StepMeta KafkaProducerMetaStep = new StepMeta(KafkaProducerPluginId, "kafka output", kafkaProducer);
        KafkaProducerMetaStep.setDraw(true);
        KafkaProducerMetaStep.setLocation(800, 300);
        transMeta.addStep(KafkaProducerMetaStep);
        // 添加hop把步骤关联起来, 相当于连线
        transMeta.addTransHop(new TransHopMeta(TextInputMetaStep, javaScriptMetaStep));
        transMeta.addTransHop(new TransHopMeta(javaScriptMetaStep, KafkaProducerMetaStep));
        System.out.println("***********the end************");
        return transMeta;
    }
}