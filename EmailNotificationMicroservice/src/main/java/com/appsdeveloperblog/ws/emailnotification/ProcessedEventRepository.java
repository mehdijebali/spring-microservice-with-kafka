package com.appsdeveloperblog.ws.emailnotification;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {
	
	ProcessedEventEntity findByMessageId(String messageId);

}
