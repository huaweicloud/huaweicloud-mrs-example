package com.example.stream.load.dorisstreamloadexample;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamLoadRespContent {
	@JsonProperty(value = "TxnId")
	private Long txnId;

	@JsonProperty(value = "Label")
	private String label;

	@JsonProperty(value = "Status")
	private String status;

	@JsonProperty(value = "TwoPhaseCommit")
	private String twoPhaseCommit;

	@JsonProperty(value = "ExistingJobStatus")
	private String existingJobStatus;

	@JsonProperty(value = "Message")
	private String message;

	@JsonProperty(value = "NumberTotalRows")
	private Long numberTotalRows;

	@JsonProperty(value = "NumberLoadedRows")
	private Long numberLoadedRows;

	@JsonProperty(value = "NumberFilteredRows")
	private Integer numberFilteredRows;

	@JsonProperty(value = "NumberUnselectedRows")
	private Integer numberUnselectedRows;

	@JsonProperty(value = "LoadBytes")
	private Long loadBytes;

	@JsonProperty(value = "LoadTimeMs")
	private Integer loadTimeMs;

	@JsonProperty(value = "BeginTxnTimeMs")
	private Integer beginTxnTimeMs;

	@JsonProperty(value = "StreamLoadPutTimeMs")
	private Integer streamLoadPutTimeMs;

	@JsonProperty(value = "ReadDataTimeMs")
	private Integer readDataTimeMs;

	@JsonProperty(value = "WriteDataTimeMs")
	private Integer writeDataTimeMs;

	@JsonProperty(value = "CommitAndPublishTimeMs")
	private Integer commitAndPublishTimeMs;

	@JsonProperty(value = "ErrorURL")
	private String errorURL;

	public Long getTxnId() {
		return txnId;
	}

	public String getStatus() {
		return status;
	}

	public String getTwoPhaseCommit() {
		return twoPhaseCommit;
	}

	public String getMessage() {
		return message;
	}

	public String getExistingJobStatus() {
		return existingJobStatus;
	}

	public Long getNumberTotalRows() {
		return numberTotalRows;
	}

	public Long getNumberLoadedRows() {
		return numberLoadedRows;
	}

	public Integer getNumberFilteredRows() {
		return numberFilteredRows;
	}

	public Integer getNumberUnselectedRows() {
		return numberUnselectedRows;
	}

	public Long getLoadBytes() {
		return loadBytes;
	}

	public Integer getLoadTimeMs() {
		return loadTimeMs;
	}

	public Integer getBeginTxnTimeMs() {
		return beginTxnTimeMs;
	}

	public Integer getStreamLoadPutTimeMs() {
		return streamLoadPutTimeMs;
	}

	public Integer getReadDataTimeMs() {
		return readDataTimeMs;
	}

	public Integer getWriteDataTimeMs() {
		return writeDataTimeMs;
	}

	public Integer getCommitAndPublishTimeMs() {
		return commitAndPublishTimeMs;
	}

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return "";
		}
	}

	public String getErrorURL() {
		return errorURL;
	}
}
