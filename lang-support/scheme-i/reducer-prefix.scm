; Copyright 2010 WebMapReduce Developers
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;	http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(include "common.scm")
(require srfi/41)  ; streams library

;; library for writing reducers goes here:

(define _next_pair_
  (lambda ()
    (let ((line (read-line)))
      (if (eof-object? line) '()
          (_wmr_split_ line)))))

(define _make_wmr_reducer_input_object_
  (lambda ()
    (let ((curr (list void void)))
      (set! curr (_next_pair_))
      (lambda (method . args)
        (case method
          ((has-next) ; zero or one arg, key to match
           (if (null? curr) #f
               ;; a value is available
               (or (null? args) (equal? (car args) (car curr)))))
          ((get-key) (if (null? curr) #f (car curr)))
          ((get-next) ; one arg, key to match
           (cond 
            ((null? curr) #f)
            ;; a value is available
            ((equal? (car args) (car curr)) 
             (let ((val (cadr curr)))
               (set! curr (_next_pair_))
               val))
            ;; next value does not match arg1
            (else #f)))
          (else "No such method"))))))
  

(define _make_wmr_stream_
  (stream-lambda (key input-object)
    (if (not (input-object 'has-next key))
        stream-null
        (stream-cons
          (input-object 'get-next key)
          (_make_wmr_stream_ key input-object)))))

