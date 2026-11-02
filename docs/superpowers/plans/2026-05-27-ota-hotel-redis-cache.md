# OTA Hotel Redis Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Chapter 3 `ota-hotel` with legacy production hotel/room-type fields and Redis Cache-Aside detail caching.

**Architecture:** Add a Spring Boot 3 service module on port 8083. Use MyBatis-Plus for `hotel` and `room_type`, model fields from `/Users/fuxinwei/work/db/api_hotel.txt`, and cache hotel details by primary id with empty-value caching, mutex lock protection, TTL jitter, and cache eviction after update.

**Tech Stack:** Java 21, Spring Boot 3.5, Spring Cloud Alibaba Nacos, MyBatis-Plus, MySQL, Spring Data Redis, JUnit 5, Mockito.

---

### Task 1: Module And Database Shape

**Files:**
- Modify: `/Users/fuxinwei/work/ota-cloud/pom.xml`
- Create: `/Users/fuxinwei/work/ota-cloud/ota-hotel/pom.xml`
- Modify: `/Users/fuxinwei/work/ota-cloud/init-sql/001-init.sql`
- Create: `/Users/fuxinwei/work/ota-cloud/ota-hotel/src/main/resources/application.yml`
- Create: `/Users/fuxinwei/work/ota-cloud/ota-hotel/src/main/resources/bootstrap.yml`

- [ ] Add `ota-hotel` to the parent module list.
- [ ] Add a module POM using current `com.ota.cloud` coordinates and existing managed dependency versions.
- [ ] Add `hotel` and `room_type` tables based on legacy `api_hotel` DDL, with a small sample data set.
- [ ] Configure port `8083`, Nacos, Redis, MySQL, MyBatis-Plus, and virtual threads.

### Task 2: Cache Core With Tests

**Files:**
- Test: `/Users/fuxinwei/work/ota-cloud/ota-hotel/src/test/java/com/ota/cloud/hotel/service/CacheServiceTest.java`
- Create: `/Users/fuxinwei/work/ota-cloud/ota-hotel/src/main/java/com/ota/cloud/hotel/service/CacheService.java`

- [ ] Write tests for cache hit, DB load on miss, null-value caching, and explicit eviction.
- [ ] Run the test and confirm it fails before `CacheService` exists.
- [ ] Implement `CacheService` with Cache-Aside, `NULL` marker, `lock:` mutex, TTL jitter, and value-checked unlock.
- [ ] Run the test and confirm it passes.

### Task 3: Hotel Domain And Queries

**Files:**
- Create entity, mapper, XML, DTO/VO, service, controller, and config files under `/Users/fuxinwei/work/ota-cloud/ota-hotel/src/main`.

- [ ] Create legacy-shaped `Hotel` and `RoomType` entities.
- [ ] Create list/search/detail query mappers.
- [ ] Create `HotelService` that joins room types by `partner_code + hotel_id`, caches detail, and evicts after update.
- [ ] Create `HotelController` endpoints from Chapter 3.
- [ ] Add virtual-thread inspection endpoint.

### Task 4: Verification And Progress

**Files:**
- Modify: `/Users/fuxinwei/work/ota-cloud/cloud-tutorial-progress.md`
- Modify: `/Users/fuxinwei/work/ota-cloud/README.md`

- [ ] Run module tests.
- [ ] Run full `mvn -q -DskipTests compile`.
- [ ] Update progress and README with Chapter 3 notes and curl examples.
