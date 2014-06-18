#!/usr/bin/env ruby
# encoding: utf-8

# Please note that this script should be executed within
# the YARN application context in such a manner as
#   dmchk@tazik:~/Work/yarn$ rails r putpack.rb

require 'csv'
require 'pry'

Dir.chdir(File.expand_path('../', __FILE__)) do
  Word.transaction do
    CSV.foreach('current_words.cleaned.csv', headers: true) do |row|
      next if row['word'].blank?
      Word.where(id: row['id']).update_all(word: row['word'])
    end
  end

  Definition.transaction do
    CSV.foreach('current_definitions.cleaned.csv', headers: true) do |row|
      next if row['text'].blank?
      Definition.where(id: row['id']).update_all(source: row['source'], text: row['text'])
    end
  end

  Example.transaction do
    CSV.foreach('current_examples.cleaned.csv', headers: true) do |row|
      next if row['text'].blank?
      Example.where(id: row['id']).update_all(source: row['source'], text: row['text'])
    end
  end
end
