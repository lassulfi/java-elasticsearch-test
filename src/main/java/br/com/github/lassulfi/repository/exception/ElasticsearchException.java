package br.com.github.lassulfi.repository.exception;

public class ElasticsearchException extends Exception {

	private static final long serialVersionUID = 7602605667403245758L;

	public ElasticsearchException(String msg) {
		super(msg);
	}
	
	
	public ElasticsearchException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
