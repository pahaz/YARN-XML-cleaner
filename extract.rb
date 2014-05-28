#!/usr/bin/env ruby
# encoding: utf-8

# Please note that this script should be executed within
# the YARN application context in such a manner as
#   dmchk@tazik:~/Work/yarn$ rails r extract.rb

require 'csv'

class String
  def nobr
    gsub(/[\n\r\u2028]+/, ' ').tap(&:strip!)
  end
end

CSV.open('current_words.csv', 'w') do |csv|
  csv << %w(id word)
  Word.find_each do |w|
    csv << [w.id, w.word.nobr]
  end
end

CSV.open('current_definitions.csv', 'w') do |csv|
  csv << %w(id source text)
  Definition.find_each do |d|
    csv << [d.id, d.try(:source).try(:nobr), d.try(:text).try(:nobr)]
  end
end

CSV.open('current_examples.csv', 'w') do |csv|
  csv << %w(id source text)
  Example.find_each do |x|
    csv << [x.id, x.try(:source).try(:nobr), x.try(:text).try(:nobr)]
  end
end
